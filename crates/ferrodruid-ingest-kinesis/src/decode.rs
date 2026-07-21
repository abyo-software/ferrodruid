// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Record decode: Kinesis record payload bytes → JSON row(s).
//!
//! v1 supports one input format — **JSON objects**. A single record may
//! carry ONE object or MULTIPLE whitespace/newline-separated (or
//! concatenated) objects: producers commonly batch several events into
//! one record, Druid's `json` inputFormat (`JsonReader`) reads each as
//! its own row, and the shared `StreamingBuffer`'s `push_payload`
//! ingests exactly the same shapes — so this decode must accept
//! everything the ingester itself accepts (ship2 H7: a pre-filter
//! stricter than the ingester dead-lettered producer-batched records
//! the buffer would have stored, permanently losing them once the
//! frontier committed past). A record that fails to decode is a LOUD,
//! per-record dead letter ([`DeadLetter`], `tracing::warn!`-ed with its
//! shard / sequence / partition key) — never a panic and never a silent
//! drop, mirroring the fail-closed philosophy of the Kafka path.

use crate::source::KinesisRecord;

/// Why one record failed to decode.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecordDecodeError {
    /// The payload bytes are not valid UTF-8.
    #[error("record data is not valid UTF-8: {0}")]
    Utf8(String),
    /// The payload is not valid JSON.
    #[error("record data is not valid JSON: {0}")]
    Json(String),
    /// The payload is valid JSON but not an OBJECT — a row must be a
    /// JSON object mapping column names to values. In a multi-object
    /// record EVERY object must be a JSON object; one non-object value
    /// fails the whole record (all-or-nothing, matching `push_payload`).
    #[error("record data is valid JSON but not an object (got {0})")]
    NotAnObject(&'static str),
    /// The payload contained no JSON value at all (empty or
    /// whitespace-only), matching the shared buffer's refusal of a
    /// value-less record.
    #[error("record contained no JSON value")]
    Empty,
}

/// A record that failed to decode, with enough provenance to find it in
/// the stream (shard + sequence number are the record's coordinates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    /// Shard the record came from.
    pub shard_id: String,
    /// The record's sequence number (its coordinate within the shard).
    pub sequence_number: String,
    /// The producer-chosen partition key.
    pub partition_key: String,
    /// Human-readable decode failure.
    pub error: String,
}

/// The outcome of decoding one [`KinesisSource`](crate::source::KinesisSource)
/// batch: rows ready for the ingest buffer, plus the per-record dead
/// letters (already warn-logged by [`decode_batch`]).
#[derive(Debug, Clone, Default)]
pub struct DecodedBatch {
    /// Successfully decoded JSON object rows, in shard order (a
    /// multi-object record contributes each of its objects, in order).
    pub rows: Vec<serde_json::Value>,
    /// Records that failed to decode (loud, never silent).
    pub dead_letters: Vec<DeadLetter>,
}

/// Decode ONE record's payload bytes into its JSON object row(s).
///
/// A record may carry multiple whitespace-separated (or concatenated)
/// JSON objects — the Druid `json` inputFormat shape the shared
/// `StreamingBuffer` ingests (ship2 H7) — and ALL of them are returned,
/// in payload order. All-or-nothing: one malformed / non-object value
/// fails the WHOLE record, exactly as `push_payload` refuses it, so
/// this pre-filter can never pass a record the buffer's own parse
/// rejects nor reject a shape it accepts.
///
/// # Errors
/// [`RecordDecodeError`] if the bytes are not UTF-8, any value is not
/// JSON or not a JSON object, or the payload contains no JSON value.
pub fn decode_record(record: &KinesisRecord) -> Result<Vec<serde_json::Value>, RecordDecodeError> {
    let text =
        std::str::from_utf8(&record.data).map_err(|e| RecordDecodeError::Utf8(e.to_string()))?;
    let mut rows = Vec::new();
    let mut stream = serde_json::Deserializer::from_str(text).into_iter::<serde_json::Value>();
    for item in &mut stream {
        let value = item.map_err(|e| RecordDecodeError::Json(e.to_string()))?;
        if !value.is_object() {
            return Err(RecordDecodeError::NotAnObject(json_type_name(&value)));
        }
        rows.push(value);
    }
    if rows.is_empty() {
        return Err(RecordDecodeError::Empty);
    }
    Ok(rows)
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Decode a whole [`get_records`](crate::source::KinesisSource::get_records)
/// batch from one shard: good rows in, decode failures out as LOUD
/// per-record dead letters (each `tracing::warn!`-ed with shard /
/// sequence / partition-key provenance). Never panics.
#[must_use]
pub fn decode_batch(shard_id: &str, records: &[KinesisRecord]) -> DecodedBatch {
    let mut batch = DecodedBatch::default();
    for record in records {
        match decode_record(record) {
            Ok(rows) => batch.rows.extend(rows),
            Err(err) => {
                let error = err.to_string();
                tracing::warn!(
                    shard_id,
                    sequence_number = %record.sequence_number,
                    partition_key = %record.partition_key,
                    error = %error,
                    "kinesis record failed to decode — dead-lettered (the \
                     record is NOT ingested and NOT retried; fix the producer \
                     or the spec's input format)"
                );
                batch.dead_letters.push(DeadLetter {
                    shard_id: shard_id.to_owned(),
                    sequence_number: record.sequence_number.clone(),
                    partition_key: record.partition_key.clone(),
                    error,
                });
            }
        }
    }
    batch
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(data: &[u8]) -> KinesisRecord {
        KinesisRecord {
            partition_key: "pk-1".to_owned(),
            sequence_number: "49590000000000000000000000000000000000000000000000000001".to_owned(),
            data: data.to_vec(),
            approximate_arrival_millis: Some(1_700_000_000_000),
        }
    }

    #[test]
    fn decode_json_object_ok() {
        let rec = record(br#"{"ts": 1700000000000, "user": "alice", "n": 3}"#);
        let rows = decode_record(&rec).expect("decode");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["user"], "alice");
        assert_eq!(rows[0]["n"], 3);
    }

    /// Ship2 H7: a producer-batched record carrying multiple
    /// newline-separated (or concatenated / whitespace-separated) JSON
    /// objects decodes to ALL its rows, in order — the shapes Druid's
    /// `json` inputFormat and the shared buffer's `push_payload` accept.
    #[test]
    fn decode_multi_object_record_yields_every_row() {
        for data in [
            "{\"a\":1}\n{\"a\":2}\n{\"a\":3}".as_bytes(),
            br#"{"a":1}{"a":2}{"a":3}"#.as_slice(),
            br#"  {"a":1}   {"a":2}
                {"a":3}  "#
                .as_slice(),
        ] {
            let rows = decode_record(&record(data)).expect("decode multi");
            assert_eq!(rows.len(), 3, "payload: {data:?}");
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row["a"], i as u64 + 1);
            }
        }
    }

    /// All-or-nothing (matching `push_payload`): one malformed or
    /// non-object value fails the WHOLE multi-object record.
    #[test]
    fn decode_multi_object_record_is_all_or_nothing() {
        assert!(matches!(
            decode_record(&record(b"{\"a\":1}\n{broken")),
            Err(RecordDecodeError::Json(_))
        ));
        match decode_record(&record(b"{\"a\":1}\n[1,2]")) {
            Err(RecordDecodeError::NotAnObject(k)) => assert_eq!(k, "array"),
            other => panic!("expected NotAnObject(array), got {other:?}"),
        }
    }

    #[test]
    fn decode_empty_or_whitespace_payload_is_error() {
        for data in [b"".as_slice(), b"   \n\t  ".as_slice()] {
            assert_eq!(
                decode_record(&record(data)),
                Err(RecordDecodeError::Empty),
                "payload: {data:?}"
            );
        }
    }

    #[test]
    fn decode_invalid_utf8_is_error() {
        let rec = record(&[0xff, 0xfe, 0x00, 0x41]);
        assert!(matches!(
            decode_record(&rec),
            Err(RecordDecodeError::Utf8(_))
        ));
    }

    #[test]
    fn decode_invalid_json_is_error() {
        let rec = record(b"{not json");
        assert!(matches!(
            decode_record(&rec),
            Err(RecordDecodeError::Json(_))
        ));
    }

    #[test]
    fn decode_non_object_json_is_error() {
        for (data, kind) in [
            (br#"[1,2,3]"#.as_slice(), "array"),
            (br#""hello""#.as_slice(), "string"),
            (b"42".as_slice(), "number"),
            (b"null".as_slice(), "null"),
        ] {
            match decode_record(&record(data)) {
                Err(RecordDecodeError::NotAnObject(k)) => assert_eq!(k, kind),
                other => panic!("expected NotAnObject({kind}), got {other:?}"),
            }
        }
    }

    #[test]
    fn decode_batch_partitions_rows_and_dead_letters() {
        let good1 = record(br#"{"a": 1}"#);
        let bad = KinesisRecord {
            partition_key: "pk-bad".to_owned(),
            sequence_number: "777".to_owned(),
            data: b"{broken".to_vec(),
            approximate_arrival_millis: None,
        };
        let good2 = record(br#"{"a": 2}"#);
        let batch = decode_batch("shard-x", &[good1, bad, good2]);
        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0]["a"], 1);
        assert_eq!(batch.rows[1]["a"], 2);
        assert_eq!(batch.dead_letters.len(), 1);
        let dl = &batch.dead_letters[0];
        assert_eq!(dl.shard_id, "shard-x");
        assert_eq!(dl.sequence_number, "777");
        assert_eq!(dl.partition_key, "pk-bad");
        assert!(!dl.error.is_empty());
    }

    /// Ship2 H7: a multi-object record contributes EVERY object to the
    /// batch's rows (not a dead letter).
    #[test]
    fn decode_batch_expands_multi_object_records() {
        let single = record(br#"{"a": 1}"#);
        let multi = record(b"{\"a\": 2}\n{\"a\": 3}");
        let batch = decode_batch("shard-x", &[single, multi]);
        assert!(batch.dead_letters.is_empty(), "{:?}", batch.dead_letters);
        let values: Vec<_> = batch.rows.iter().map(|r| r["a"].clone()).collect();
        assert_eq!(values, vec![json!(1), json!(2), json!(3)]);
    }
}
