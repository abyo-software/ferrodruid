// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Record decode: Kinesis record payload bytes → JSON row.
//!
//! v1 supports one input format — **JSON, one object per record** (the
//! shape the shared `StreamingBuffer` / `BatchIngester` consumes). A
//! record that fails to decode is a LOUD, per-record dead letter
//! ([`DeadLetter`], `tracing::warn!`-ed with its shard / sequence /
//! partition key) — never a panic and never a silent drop, mirroring
//! the fail-closed philosophy of the Kafka path.

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
    /// JSON object mapping column names to values.
    #[error("record data is valid JSON but not an object (got {0})")]
    NotAnObject(&'static str),
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
    /// Successfully decoded JSON object rows, in shard order.
    pub rows: Vec<serde_json::Value>,
    /// Records that failed to decode (loud, never silent).
    pub dead_letters: Vec<DeadLetter>,
}

/// Decode ONE record's payload bytes into a JSON object row.
///
/// # Errors
/// [`RecordDecodeError`] if the bytes are not UTF-8, not JSON, or not a
/// JSON object.
pub fn decode_record(record: &KinesisRecord) -> Result<serde_json::Value, RecordDecodeError> {
    let text =
        std::str::from_utf8(&record.data).map_err(|e| RecordDecodeError::Utf8(e.to_string()))?;
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| RecordDecodeError::Json(e.to_string()))?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(RecordDecodeError::NotAnObject(json_type_name(&value)))
    }
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
            Ok(row) => batch.rows.push(row),
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
        let row = decode_record(&rec).expect("decode");
        assert_eq!(row["user"], "alice");
        assert_eq!(row["n"], 3);
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
}
