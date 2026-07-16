// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Parquet input format support for batch ingestion.
//!
//! Reads a Parquet file (or in-memory byte buffer) into the row representation
//! used by the rest of the ingestion pipeline: one [`serde_json::Value`] object
//! per record, mapping top-level column name to value. The resulting rows are
//! fed to [`crate::BatchIngester::ingest`] for timestamp / dimension / metric
//! extraction.
//!
//! Druid wire name: `{"type":"parquet"}`.
//!
//! # Supported types
//!
//! Top-level columns of the following Arrow types are mapped to JSON values:
//! boolean, all integer widths (i8..i64, u8..u64), float32/float64,
//! Utf8 / LargeUtf8 strings, and `Date32`/timestamp logical types (rendered as
//! epoch-based integers). Nulls map to `serde_json::Value::Null`.
//!
//! # Limitations
//!
//! Nested columns (structs / lists / maps) and Parquet `flattenSpec` JSON-path
//! extraction are **not** supported; such columns are skipped. Decimal columns
//! are surfaced as their underlying integer representation without applying the
//! scale. See the crate-level limitations notes.

use std::collections::BTreeMap;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use ferrodruid_common::error::{DruidError, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use bytes::Bytes;

/// Maximum number of rows a single Parquet parse may accumulate into memory.
///
/// A crafted-but-valid Parquet file can decode into an arbitrarily large number
/// of rows; since every row is materialized into one in-memory
/// [`serde_json::Value`], an unbounded accumulation is a resource-exhaustion
/// (OOM) hazard. This cap bounds the worst case. It mirrors the `MAX_*` style
/// used by the segment column decoders (`MAX_STRING_COLUMN_ROWS`). The default
/// (64 Mi rows) is generous for legitimate batch files while still bounding RAM
/// to a few GiB in the pathological case. Exceeding it is reported as an
/// [`DruidError::Ingestion`] rather than allowed to OOM the process.
const MAX_PARQUET_ROWS: usize = 64 * 1024 * 1024;

/// Parse a Parquet byte buffer into ingestion rows.
///
/// Each returned [`serde_json::Value`] is a JSON object keyed by top-level
/// column name. Columns whose Arrow type is not representable as a scalar JSON
/// value (nested structs/lists/maps) are skipped.
///
/// Accumulation is bounded by [`MAX_PARQUET_ROWS`]; a file decoding to more
/// rows than the cap is rejected (see Errors).
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the buffer is not a valid Parquet file,
/// a record batch cannot be read, or the decoded row count exceeds
/// [`MAX_PARQUET_ROWS`].
pub fn parse_parquet_bytes(data: Vec<u8>) -> Result<Vec<serde_json::Value>> {
    parse_parquet_bytes_capped(data, MAX_PARQUET_ROWS)
}

/// Parse a Parquet byte buffer with an explicit row cap.
///
/// Behaves like [`parse_parquet_bytes`] but uses `max_rows` as the accumulation
/// bound, allowing the cap to be exercised cheaply in tests.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the buffer is not a valid Parquet file,
/// a record batch cannot be read, or the decoded row count exceeds `max_rows`.
fn parse_parquet_bytes_capped(data: Vec<u8>, max_rows: usize) -> Result<Vec<serde_json::Value>> {
    // The upstream `parquet` crate (v55.2.0) can *panic* — rather than return an
    // `Err` — on a malformed thrift-encoded file-metadata footer (e.g. a slice
    // index out of bounds in `thrift.rs` while skipping an unknown field during
    // `try_new`/decode). Such a panic unwinds straight through the `.map_err`
    // calls below, so our error mapping never runs and the ingestion task is
    // aborted — a DoS reachable from an untrusted Parquet upload. We therefore
    // run the whole parse inside `catch_unwind` and convert any caught panic
    // into a `DruidError::Ingestion`.
    //
    // `AssertUnwindSafe` is sound here: the closure operates only on owned,
    // local data (the input bytes and a freshly-allocated `rows` vector). It
    // shares no state with the caller across the boundary, so a panic mid-parse
    // cannot leave any observable value in a broken invariant — the partially
    // built `rows` is dropped and never returned.
    //
    // Note: `catch_unwind` only catches *unwinding* panics; this requires
    // `panic = "unwind"` (the Rust default for dev/test/release — confirmed: no
    // profile in the workspace sets `panic = "abort"`). It does not and cannot
    // catch allocation aborts; resource bounds are handled separately via the
    // row cap below.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parse_parquet_bytes_inner(data, max_rows)
    }));

    match result {
        Ok(rows) => rows,
        Err(_) => Err(DruidError::Ingestion(
            "malformed parquet file: decoder panicked while reading metadata/data \
             (rejected to avoid aborting the ingestion task)"
                .to_string(),
        )),
    }
}

/// Inner Parquet parse, run inside [`std::panic::catch_unwind`] by
/// [`parse_parquet_bytes_capped`]. Operates only on owned local data.
fn parse_parquet_bytes_inner(data: Vec<u8>, max_rows: usize) -> Result<Vec<serde_json::Value>> {
    let bytes = Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| DruidError::Ingestion(format!("failed to open parquet buffer: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| DruidError::Ingestion(format!("failed to build parquet reader: {e}")))?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch_res in reader {
        let batch = batch_res
            .map_err(|e| DruidError::Ingestion(format!("parquet batch read error: {e}")))?;
        if rows.len().saturating_add(batch.num_rows()) > max_rows {
            return Err(DruidError::Ingestion(format!(
                "parquet file exceeds row cap of {max_rows}; refusing to accumulate further \
                 (potential resource exhaustion)"
            )));
        }
        append_batch_rows(&batch, &mut rows)?;
    }
    Ok(rows)
}

/// Read a Parquet file from disk into ingestion rows.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the file cannot be read or parsed.
pub fn parse_parquet_file(path: &std::path::Path) -> Result<Vec<serde_json::Value>> {
    let data = std::fs::read(path).map_err(|e| {
        DruidError::Ingestion(format!(
            "failed to read parquet file {}: {e}",
            path.display()
        ))
    })?;
    parse_parquet_bytes(data)
}

/// Append every row of an Arrow [`RecordBatch`] to `rows`.
fn append_batch_rows(batch: &RecordBatch, rows: &mut Vec<serde_json::Value>) -> Result<()> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    for row_idx in 0..num_rows {
        let mut obj = serde_json::Map::new();
        for col_idx in 0..num_cols {
            let field = schema.field(col_idx);
            let name = field.name();
            let array = batch.column(col_idx);
            if let Some(value) = scalar_at(array.as_ref(), row_idx)? {
                obj.insert(name.clone(), value);
            }
            // Columns that produce no scalar (nested / unsupported) are skipped.
        }
        rows.push(serde_json::Value::Object(obj));
    }
    Ok(())
}

/// Whether a column's Arrow [`DataType`] is representable as a scalar JSON
/// value by [`scalar_at`].
///
/// Used to decide column presence *uniformly* across rows: an unsupported
/// column is skipped for both null and non-null cells, so a column never
/// appears in some rows (as `Null`) yet vanishes in others.
fn is_supported_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
    )
}

/// Extract a single cell as a JSON value.
///
/// Returns `Ok(None)` for column types that are not representable as a scalar
/// JSON value (nested structs/lists/maps) — uniformly for both null and
/// non-null cells, so column presence is consistent across rows. Returns
/// `Ok(Some(Null))` for nulls in otherwise-supported columns.
fn scalar_at(array: &dyn Array, idx: usize) -> Result<Option<serde_json::Value>> {
    // Decide presence by column type first, so an unsupported column is skipped
    // for *every* row regardless of whether the individual cell is null.
    if !is_supported_type(array.data_type()) {
        return Ok(None);
    }

    if array.is_null(idx) {
        return Ok(Some(serde_json::Value::Null));
    }

    let value = match array.data_type() {
        DataType::Boolean => {
            downcast::<BooleanArray>(array)?.map(|a| serde_json::Value::Bool(a.value(idx)))
        }
        DataType::Int8 => downcast::<Int8Array>(array)?.map(|a| json_i64(i64::from(a.value(idx)))),
        DataType::Int16 => {
            downcast::<Int16Array>(array)?.map(|a| json_i64(i64::from(a.value(idx))))
        }
        DataType::Int32 => {
            downcast::<Int32Array>(array)?.map(|a| json_i64(i64::from(a.value(idx))))
        }
        DataType::Int64 => downcast::<Int64Array>(array)?.map(|a| json_i64(a.value(idx))),
        DataType::UInt8 => {
            downcast::<UInt8Array>(array)?.map(|a| json_u64(u64::from(a.value(idx))))
        }
        DataType::UInt16 => {
            downcast::<UInt16Array>(array)?.map(|a| json_u64(u64::from(a.value(idx))))
        }
        DataType::UInt32 => {
            downcast::<UInt32Array>(array)?.map(|a| json_u64(u64::from(a.value(idx))))
        }
        DataType::UInt64 => downcast::<UInt64Array>(array)?.map(|a| json_u64(a.value(idx))),
        DataType::Float32 => {
            downcast::<Float32Array>(array)?.map(|a| json_f64(f64::from(a.value(idx))))
        }
        DataType::Float64 => downcast::<Float64Array>(array)?.map(|a| json_f64(a.value(idx))),
        DataType::Utf8 => downcast::<StringArray>(array)?
            .map(|a| serde_json::Value::String(a.value(idx).to_string())),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?
            .map(|a| serde_json::Value::String(a.value(idx).to_string())),
        DataType::Date32 => {
            downcast::<Date32Array>(array)?.map(|a| json_i64(i64::from(a.value(idx))))
        }
        DataType::Timestamp(unit, _) => timestamp_millis(array, idx, *unit)?.map(json_i64),
        // Nested / unsupported types are skipped.
        _ => return Ok(None),
    };

    Ok(value)
}

/// Convert a timestamp cell to epoch milliseconds.
fn timestamp_millis(array: &dyn Array, idx: usize, unit: TimeUnit) -> Result<Option<i64>> {
    let millis = match unit {
        TimeUnit::Second => {
            downcast::<TimestampSecondArray>(array)?.map(|a| a.value(idx).saturating_mul(1000))
        }
        TimeUnit::Millisecond => {
            downcast::<TimestampMillisecondArray>(array)?.map(|a| a.value(idx))
        }
        TimeUnit::Microsecond => {
            downcast::<TimestampMicrosecondArray>(array)?.map(|a| a.value(idx) / 1000)
        }
        TimeUnit::Nanosecond => {
            downcast::<TimestampNanosecondArray>(array)?.map(|a| a.value(idx) / 1_000_000)
        }
    };
    Ok(millis)
}

/// Downcast an [`Array`] to a concrete array type, erroring (never panicking)
/// if the dynamic type does not match.
fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<Option<&T>> {
    match array.as_any().downcast_ref::<T>() {
        Some(a) => Ok(Some(a)),
        None => Err(DruidError::Ingestion(format!(
            "parquet column downcast failed for {:?}",
            array.data_type()
        ))),
    }
}

fn json_i64(v: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

fn json_u64(v: u64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

fn json_f64(v: f64) -> serde_json::Value {
    match serde_json::Number::from_f64(v) {
        Some(n) => serde_json::Value::Number(n),
        // Non-finite floats (NaN / inf) have no JSON number representation.
        None => serde_json::Value::Null,
    }
}

/// Collect the set of top-level column names present across all parsed rows.
///
/// Useful for callers that want to discover the schema without re-reading the
/// Parquet metadata.
#[must_use]
pub fn column_names(rows: &[serde_json::Value]) -> Vec<String> {
    let mut set: BTreeMap<String, ()> = BTreeMap::new();
    for row in rows {
        if let serde_json::Value::Object(map) = row {
            for k in map.keys() {
                set.insert(k.clone(), ());
            }
        }
    }
    set.into_keys().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    /// Write a small parquet buffer with mixed types and a null, then parse it.
    fn write_sample_parquet() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("count", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        let ts: ArrayRef = Arc::new(Int64Array::from(vec![1000_i64, 2000, 3000]));
        let region: ArrayRef = Arc::new(StringArray::from(vec![Some("us"), None, Some("eu")]));
        let count: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
        let revenue: ArrayRef = Arc::new(Float64Array::from(vec![1.5_f64, 2.5, 3.5]));
        let active: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));

        let batch = RecordBatch::try_new(schema.clone(), vec![ts, region, count, revenue, active])
            .expect("record batch");

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        buf
    }

    #[test]
    fn parse_parquet_roundtrip_types() {
        let buf = write_sample_parquet();
        let rows = parse_parquet_bytes(buf).expect("parse");
        assert_eq!(rows.len(), 3);

        // Row 0
        assert_eq!(rows[0]["ts"], serde_json::json!(1000));
        assert_eq!(rows[0]["region"], serde_json::json!("us"));
        assert_eq!(rows[0]["count"], serde_json::json!(10));
        assert_eq!(rows[0]["revenue"], serde_json::json!(1.5));
        assert_eq!(rows[0]["active"], serde_json::json!(true));

        // Row 1 has a null region
        assert_eq!(rows[1]["region"], serde_json::Value::Null);
        assert_eq!(rows[1]["active"], serde_json::json!(false));

        // Row 2
        assert_eq!(rows[2]["region"], serde_json::json!("eu"));
        assert_eq!(rows[2]["revenue"], serde_json::json!(3.5));
    }

    #[test]
    fn parse_parquet_column_names() {
        let buf = write_sample_parquet();
        let rows = parse_parquet_bytes(buf).expect("parse");
        let names = column_names(&rows);
        assert_eq!(names, vec!["active", "count", "region", "revenue", "ts"]);
    }

    #[test]
    fn parse_parquet_feeds_ingester() {
        let buf = write_sample_parquet();
        let rows = parse_parquet_bytes(buf).expect("parse");

        let ingester = crate::BatchIngester::new(
            "ds".into(),
            "ts".into(),
            vec!["region".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "revenue"})],
        );
        let result = ingester.ingest(rows).expect("ingest parquet rows");
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.segment_data.dimensions, vec!["region"]);
        assert_eq!(result.segment_data.metrics, vec!["revenue"]);
    }

    #[test]
    fn parse_parquet_invalid_buffer() {
        let err = parse_parquet_bytes(vec![0u8, 1, 2, 3]).unwrap_err();
        assert!(err.to_string().contains("parquet"));
    }

    #[test]
    fn parse_parquet_unsupported_type_presence_consistent() {
        use arrow::array::Decimal128Array;
        // A Decimal128 column is unsupported. One null row + one non-null row:
        // the key must be either present in BOTH rows or absent in BOTH —
        // never present-as-Null in one and missing in the other.
        let dec: ArrayRef = Arc::new(
            Decimal128Array::from(vec![None, Some(12345_i128)])
                .with_precision_and_scale(10, 2)
                .expect("decimal array"),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("price", dec.data_type().clone(), true),
        ]));
        let ts: ArrayRef = Arc::new(Int64Array::from(vec![1000_i64, 2000]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts, dec]).expect("batch");

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
            writer.write(&batch).expect("write");
            writer.close().expect("close");
        }

        let rows = parse_parquet_bytes(buf).expect("parse");
        assert_eq!(rows.len(), 2);
        let p0 = rows[0].get("price").is_some();
        let p1 = rows[1].get("price").is_some();
        assert_eq!(
            p0, p1,
            "unsupported column presence inconsistent across rows: row0={p0} row1={p1}"
        );
        // With the fix, the unsupported column is uniformly absent.
        assert!(!p0, "unsupported Decimal column should be skipped");
    }

    #[test]
    fn parse_parquet_row_cap_rejected() {
        // A 3-row file parsed with a cap of 2 must be rejected with an
        // Ingestion error rather than accumulating unbounded rows.
        let buf = write_sample_parquet();
        let err = parse_parquet_bytes_capped(buf, 2).unwrap_err();
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("row cap"),
                "expected row-cap error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn parse_parquet_row_cap_within_limit_ok() {
        // Exactly at the cap is allowed.
        let buf = write_sample_parquet();
        let rows = parse_parquet_bytes_capped(buf, 3).expect("parse within cap");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn parse_parquet_malformed_footer_panic_caught() {
        // Regression for fuzz crash
        // `crash-parquet-thrift-skip-slice-oob-panic`: a malformed thrift footer
        // makes the upstream `parquet` crate panic (slice index OOB) with the
        // panic unwinding through our `.map_err`. The fix wraps the parse in
        // `catch_unwind`, so this must return an Ingestion error, NOT abort.
        let crash: Vec<u8> = vec![
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00,
            0x16, 0x0e, 0x16, 0x16, 0x05, 0x16, 0x16, 0x72, 0x16, 0x16, 0x16, 0xda, 0xd1, 0x25,
            0x25, 0x25, 0x25, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9,
            0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0xb9, 0x00, 0x77, 0x16, 0x00, 0xaf, 0x16,
            0x16, 0x00, 0x16, 0x16, 0x16, 0x16, 0x16, 0x00, 0x00, 0x00, 0x50, 0x41, 0x52, 0x31,
        ];
        // Silence the default panic hook so the (caught) upstream panic does not
        // print a backtrace into the test log.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = parse_parquet_bytes(crash);
        std::panic::set_hook(prev);

        match result {
            Err(DruidError::Ingestion(_)) => {}
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn parse_parquet_file_roundtrip() {
        let buf = write_sample_parquet();
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("data.parquet");
        std::fs::write(&path, &buf).expect("write");
        let rows = parse_parquet_file(&path).expect("parse file");
        assert_eq!(rows.len(), 3);
    }
}
