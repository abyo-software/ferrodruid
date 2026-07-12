// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real Kafka consumer task that reads from a topic and produces segments.
//!
//! The buffer/flush logic works without the `kafka-io` feature; actual Kafka I/O
//! is gated behind `#[cfg(feature = "kafka-io")]`.

#[cfg(feature = "kafka-io")]
use rdkafka::Message;
#[cfg(feature = "kafka-io")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "kafka-io")]
use rdkafka::consumer::{Consumer, StreamConsumer};

use std::collections::HashMap;
use std::path::PathBuf;

use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_segment::writer::write_segment_v9;
use tokio::sync::mpsc;

/// Configuration for the Kafka consumer task.
#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    /// Kafka broker addresses (comma-separated).
    pub brokers: String,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group identifier.
    pub group_id: String,
    /// Target data source name for produced segments.
    pub data_source: String,
    /// Column containing the primary timestamp.
    pub timestamp_column: String,
    /// Dimension column names to extract.
    pub dimensions: Vec<String>,
    /// Maximum rows per segment before flushing.
    pub max_rows_per_segment: usize,
    /// Flush interval in milliseconds (for time-based flushing).
    pub segment_flush_interval_ms: u64,
    /// Whether to start from the earliest available offset.
    pub use_earliest_offset: bool,
    /// Additional Kafka consumer properties.
    pub additional_properties: HashMap<String, String>,
    /// Optional directory to write flushed segments to (in smoosh v9 format).
    /// When set, each `flush()` writes one subdirectory per segment.
    pub output_dir: Option<PathBuf>,
}

/// A message consumed from Kafka.
#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    /// Message key (optional).
    pub key: Option<Vec<u8>>,
    /// Message payload.
    pub payload: Vec<u8>,
    /// Source topic.
    pub topic: String,
    /// Source partition.
    pub partition: i32,
    /// Offset within the partition.
    pub offset: i64,
    /// Message timestamp in milliseconds (optional).
    pub timestamp_ms: Option<i64>,
}

/// Result of a segment flush.
#[derive(Debug, Clone)]
pub struct SegmentFlushResult {
    /// Data source name.
    pub data_source: String,
    /// Generated segment identifier.
    pub segment_id: String,
    /// Number of rows in the flushed segment.
    pub row_count: usize,
    /// Segment interval start (epoch millis).
    pub interval_start_millis: i64,
    /// Segment interval end (epoch millis).
    pub interval_end_millis: i64,
    /// Filesystem path where the segment was persisted (only set if
    /// `KafkaConsumerConfig::output_dir` was configured).
    pub segment_path: Option<PathBuf>,
}

/// Errors during Kafka consumer task operation.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    /// Failed to parse a record payload.
    #[error("parse error: {0}")]
    ParseError(String),
    /// Attempted to flush an empty buffer.
    #[error("empty buffer")]
    EmptyBuffer,
    /// Kafka client error (requires `kafka-io` feature).
    #[error("kafka error: {0}")]
    KafkaError(String),
    /// Segment ingestion error.
    #[error("ingestion error: {0}")]
    IngestionError(String),
    /// Record payload exceeded the per-record byte cap.
    ///
    /// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2:
    /// untrusted Kafka record JSON is capped at
    /// [`MAX_RECORD_PAYLOAD_BYTES`] before any parser-side allocation.
    #[error("record payload too large: {actual} bytes > {limit} bytes")]
    PayloadTooLarge {
        /// Observed payload size in bytes.
        actual: usize,
        /// Configured limit in bytes.
        limit: usize,
    },
    /// Record JSON nesting depth exceeded the configured cap.
    ///
    /// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2:
    /// arbitrarily-nested JSON (e.g. `[[[[…]]]]`) is rejected before
    /// `serde_json` allocates a recursive `Value` tree. The cap is
    /// [`MAX_RECORD_JSON_DEPTH`].
    #[error(
        "record JSON nesting depth exceeded: limit {limit}; bailed at byte offset {byte_offset}"
    )]
    PayloadTooDeep {
        /// Configured maximum nesting depth.
        limit: usize,
        /// Byte offset within the payload where the cap was breached.
        byte_offset: usize,
    },
}

/// Maximum bytes accepted from a single Kafka record payload before
/// JSON parsing is attempted.
///
/// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2: this
/// is a defence-in-depth bound, not a strict semantic limit.  1 MiB
/// is comfortably larger than typical event-stream rows (often KB-
/// scale) but small enough that a malicious producer cannot drive the
/// indexer into OOM by streaming gigantic blobs.  Operators who legit-
/// imately need higher caps can adjust this constant locally; the
/// value is intentionally a constant rather than a config knob to keep
/// the safety-net semantics auditable.
pub const MAX_RECORD_PAYLOAD_BYTES: usize = 1_048_576;

/// Maximum JSON nesting depth accepted from a single Kafka record
/// payload.
///
/// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2: this
/// is a stack-safety bound — `serde_json` recurses on each `{` or `[`,
/// and a sufficiently nested payload can exhaust the thread stack
/// before `serde_json` itself trips its built-in recursion limiter.
/// 32 is well above any realistic Druid event row schema (typically
/// <8) but well below the depth at which release-build stacks would
/// be threatened.
pub const MAX_RECORD_JSON_DEPTH: usize = 32;

/// Pre-flight depth/length validation for a Kafka record payload.
///
/// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2.
///
/// This walks the raw bytes once, tracking string/escape state, and
/// fails fast on:
/// * a payload byte length that exceeds [`MAX_RECORD_PAYLOAD_BYTES`],
/// * a `{` or `[` opening that pushes the running depth past
///   [`MAX_RECORD_JSON_DEPTH`].
///
/// It does **not** attempt full JSON validity checking — `serde_json`
/// remains the source of truth for syntax errors.  The goal is solely
/// to refuse pathological inputs *before* `serde_json::from_slice`
/// allocates a recursive `Value` tree on attacker-supplied input.
///
/// Returns `Ok(())` when the payload is within bounds; otherwise a
/// [`ConsumerError::PayloadTooLarge`] / [`ConsumerError::PayloadTooDeep`].
pub(crate) fn check_record_bounds(bytes: &[u8]) -> Result<(), ConsumerError> {
    if bytes.len() > MAX_RECORD_PAYLOAD_BYTES {
        return Err(ConsumerError::PayloadTooLarge {
            actual: bytes.len(),
            limit: MAX_RECORD_PAYLOAD_BYTES,
        });
    }

    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (offset, &byte) in bytes.iter().enumerate() {
        if in_string {
            // Inside a JSON string literal, only `"` (when not escaped)
            // closes; `\` toggles a one-shot escape state covering the
            // next byte (which may itself be `"` and must NOT close).
            if escape_next {
                escape_next = false;
            } else if byte == b'\\' {
                escape_next = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                if depth > MAX_RECORD_JSON_DEPTH {
                    return Err(ConsumerError::PayloadTooDeep {
                        limit: MAX_RECORD_JSON_DEPTH,
                        byte_offset: offset,
                    });
                }
            }
            b'}' | b']' => {
                // Underflow is left to serde_json — we only enforce
                // an upper bound here; an unbalanced payload will fail
                // syntactic validation downstream with a precise error.
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    Ok(())
}

/// Kafka consumer task that accumulates rows and periodically flushes to segments.
pub struct KafkaConsumerTask {
    config: KafkaConsumerConfig,
    buffer: Vec<serde_json::Value>,
    flush_results: Vec<SegmentFlushResult>,
    total_consumed: u64,
    #[allow(dead_code)]
    shutdown_rx: mpsc::Receiver<()>,
}

impl KafkaConsumerTask {
    /// Create a new consumer task with the given config and shutdown channel.
    pub fn new(config: KafkaConsumerConfig, shutdown_rx: mpsc::Receiver<()>) -> Self {
        Self {
            config,
            buffer: Vec::new(),
            flush_results: Vec::new(),
            total_consumed: 0,
            shutdown_rx,
        }
    }

    /// Process a single record: parse JSON, add to buffer, flush if needed.
    ///
    /// Wave 45-C closure of Wave 37B `ingest_kafka_tail` Medium #2:
    /// untrusted Kafka record payloads are size- and depth-checked by
    /// [`check_record_bounds`] *before* `serde_json` is invoked, so a
    /// malicious producer cannot drive the indexer into OOM or stack
    /// overflow with a single gigantic / deeply-nested message.
    pub fn process_record(
        &mut self,
        record: &ConsumedRecord,
    ) -> Result<Option<SegmentFlushResult>, ConsumerError> {
        // Defence-in-depth bounds check (W37B ingest_kafka Medium #2).
        check_record_bounds(&record.payload)?;

        // Parse payload as JSON
        let row: serde_json::Value = serde_json::from_slice(&record.payload)
            .map_err(|e| ConsumerError::ParseError(e.to_string()))?;

        // Add to buffer
        self.buffer.push(row);
        self.total_consumed += 1;

        // If buffer >= max_rows_per_segment, flush
        if self.buffer.len() >= self.config.max_rows_per_segment {
            return self.flush().map(Some);
        }
        Ok(None)
    }

    /// Flush current buffer to a segment.
    pub fn flush(&mut self) -> Result<SegmentFlushResult, ConsumerError> {
        if self.buffer.is_empty() {
            return Err(ConsumerError::EmptyBuffer);
        }

        let rows = std::mem::take(&mut self.buffer);
        let row_count = rows.len();

        // Use BatchIngester to produce segment
        let ingester = BatchIngester::new(
            self.config.data_source.clone(),
            self.config.timestamp_column.clone(),
            self.config.dimensions.clone(),
            vec![], // metrics specs
        );

        let segment = ingester
            .ingest(rows)
            .map_err(|e| ConsumerError::IngestionError(e.to_string()))?;

        let segment_id = format!("{}_{}", self.config.data_source, self.total_consumed);

        // Optionally persist the segment to disk in smoosh v9 format.
        let segment_path = if let Some(base) = self.config.output_dir.as_ref() {
            let dir = base.join(&segment_id);
            write_segment_v9(&segment.segment_data, &dir)
                .map_err(|e| ConsumerError::IngestionError(e.to_string()))?;
            Some(dir)
        } else {
            None
        };

        let result = SegmentFlushResult {
            data_source: self.config.data_source.clone(),
            segment_id,
            row_count,
            interval_start_millis: segment.interval.start_millis,
            interval_end_millis: segment.interval.end_millis,
            segment_path,
        };

        self.flush_results.push(result.clone());
        Ok(result)
    }

    /// Get total consumed records.
    pub fn total_consumed(&self) -> u64 {
        self.total_consumed
    }

    /// Get number of unflushed records in buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Get all flush results.
    pub fn flush_results(&self) -> &[SegmentFlushResult] {
        &self.flush_results
    }

    /// Run the consumer loop (requires `kafka-io` feature).
    #[cfg(feature = "kafka-io")]
    pub async fn run(&mut self) -> Result<(), ConsumerError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set(
                "auto.offset.reset",
                if self.config.use_earliest_offset {
                    "earliest"
                } else {
                    "latest"
                },
            )
            .create()
            .map_err(|e| ConsumerError::KafkaError(e.to_string()))?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| ConsumerError::KafkaError(e.to_string()))?;

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    // Flush remaining buffer before shutdown
                    if !self.buffer.is_empty() {
                        let _ = self.flush();
                    }
                    break;
                }
                msg = consumer.recv() => {
                    match msg {
                        Ok(m) => {
                            if let Some(payload) = m.payload() {
                                let record = ConsumedRecord {
                                    key: m.key().map(|k| k.to_vec()),
                                    payload: payload.to_vec(),
                                    topic: m.topic().to_string(),
                                    partition: m.partition(),
                                    offset: m.offset(),
                                    timestamp_ms: m.timestamp().to_millis(),
                                };
                                let _ = self.process_record(&record);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Kafka recv error: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(max_rows: usize) -> KafkaConsumerConfig {
        KafkaConsumerConfig {
            brokers: "localhost:9092".to_string(),
            topic: "test-topic".to_string(),
            group_id: "test-group".to_string(),
            data_source: "test_ds".to_string(),
            timestamp_column: "__time".to_string(),
            dimensions: vec!["dim".to_string()],
            max_rows_per_segment: max_rows,
            segment_flush_interval_ms: 10_000,
            use_earliest_offset: true,
            additional_properties: HashMap::new(),
            output_dir: None,
        }
    }

    fn make_record(ts: i64, dim_val: &str) -> ConsumedRecord {
        let payload = serde_json::json!({
            "__time": ts,
            "dim": dim_val
        });
        ConsumedRecord {
            key: None,
            payload: serde_json::to_vec(&payload).expect("serialize"),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp_ms: Some(ts),
        }
    }

    #[test]
    fn consumer_task_process_record() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        for i in 0..5 {
            let record = make_record(1000 + i, "a");
            let result = task.process_record(&record).expect("process");
            assert!(result.is_none()); // No flush yet
        }
        assert_eq!(task.buffer_size(), 5);
        assert_eq!(task.total_consumed(), 5);
    }

    #[test]
    fn consumer_task_auto_flush() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(3), rx);

        // Process 5 records; flush should trigger at 3
        for i in 0..5 {
            let record = make_record(1000 + i, "a");
            let result = task.process_record(&record).expect("process");
            if i == 2 {
                // Third record triggers flush
                let flush = result.expect("should flush");
                assert_eq!(flush.row_count, 3);
                assert_eq!(flush.data_source, "test_ds");
            } else {
                assert!(result.is_none());
            }
        }
        // Buffer should have 2 remaining
        assert_eq!(task.buffer_size(), 2);
        assert_eq!(task.total_consumed(), 5);
        assert_eq!(task.flush_results().len(), 1);
    }

    #[test]
    fn consumer_task_manual_flush() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        for i in 0..4 {
            let record = make_record(1000 + i, "b");
            task.process_record(&record).expect("process");
        }

        let result = task.flush().expect("manual flush");
        assert_eq!(result.row_count, 4);
        assert_eq!(result.data_source, "test_ds");
        assert_eq!(task.buffer_size(), 0);
        assert_eq!(task.flush_results().len(), 1);
    }

    #[test]
    fn consumer_task_empty_flush_error() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        let err = task.flush().unwrap_err();
        assert!(matches!(err, ConsumerError::EmptyBuffer));
    }

    #[test]
    fn consumer_task_invalid_json() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        let record = ConsumedRecord {
            key: None,
            payload: b"not valid json".to_vec(),
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp_ms: None,
        };
        let err = task.process_record(&record).unwrap_err();
        assert!(matches!(err, ConsumerError::ParseError(_)));
    }

    #[test]
    fn consumer_task_total_consumed() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        for i in 0..7 {
            let record = make_record(1000 + i, "x");
            task.process_record(&record).expect("process");
        }
        assert_eq!(task.total_consumed(), 7);
    }

    // -----------------------------------------------------------------
    // Wave 45-C — Wave 37B `ingest_kafka_tail` Medium #2 closure
    // (JSON DoS bounds: max payload bytes + max nesting depth).
    // -----------------------------------------------------------------

    /// Sanity: a normal-shape Kafka record still parses untouched.  This
    /// pins that the bounds check does not introduce a regression for
    /// the happy path that the rest of this module already exercises.
    #[test]
    fn kafka_consumer_accepts_normal_record() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);
        let record = make_record(1000, "ok");
        task.process_record(&record)
            .expect("normal record accepted");
        assert_eq!(task.buffer_size(), 1);
    }

    /// W37B ingest_kafka Medium #2: payload byte size beyond the
    /// 1 MiB cap must be refused before `serde_json` allocates
    /// anything.  We construct an oversized but otherwise legal JSON
    /// object (`{"k":"a..a"}`) so the failure mode under test is the
    /// byte cap, not invalid JSON.
    #[test]
    fn kafka_consumer_rejects_oversized_record() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        let mut payload = Vec::with_capacity(MAX_RECORD_PAYLOAD_BYTES + 64);
        payload.extend_from_slice(br#"{"k":""#);
        payload.resize(payload.len() + (MAX_RECORD_PAYLOAD_BYTES + 16), b'a');
        payload.extend_from_slice(br#""}"#);
        assert!(payload.len() > MAX_RECORD_PAYLOAD_BYTES);

        let record = ConsumedRecord {
            key: None,
            payload,
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp_ms: None,
        };
        let err = task
            .process_record(&record)
            .expect_err("oversize must fail");
        match err {
            ConsumerError::PayloadTooLarge { actual, limit } => {
                assert_eq!(limit, MAX_RECORD_PAYLOAD_BYTES);
                assert!(actual > MAX_RECORD_PAYLOAD_BYTES, "actual={actual}");
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
        assert_eq!(task.buffer_size(), 0, "rejected record must not buffer");
    }

    /// W37B ingest_kafka Medium #2: deeply-nested JSON must be
    /// rejected before `serde_json` recurses through the structure.
    /// We use `MAX_RECORD_JSON_DEPTH + 8` levels of `[` to push past
    /// the cap with margin.
    #[test]
    fn kafka_consumer_rejects_deeply_nested_json() {
        let (_tx, rx) = mpsc::channel(1);
        let mut task = KafkaConsumerTask::new(make_config(100), rx);

        let nest = MAX_RECORD_JSON_DEPTH + 8;
        let mut payload = vec![b'['; nest];
        payload.extend(std::iter::repeat_n(b']', nest));

        let record = ConsumedRecord {
            key: None,
            payload,
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp_ms: None,
        };
        let err = task.process_record(&record).expect_err("deep must fail");
        match err {
            ConsumerError::PayloadTooDeep { limit, byte_offset } => {
                assert_eq!(limit, MAX_RECORD_JSON_DEPTH);
                // The first `[` past the cap is at index = MAX_RECORD_JSON_DEPTH
                // (zero-indexed) — i.e. the (cap+1)-th opener.
                assert_eq!(byte_offset, MAX_RECORD_JSON_DEPTH);
            }
            other => panic!("expected PayloadTooDeep, got {other:?}"),
        }
        assert_eq!(task.buffer_size(), 0, "rejected record must not buffer");
    }

    /// Pin the string-state machine in the bounds checker: brackets
    /// inside a JSON string literal must NOT count toward depth.  Pre-
    /// fix a naive byte scanner would count `[` inside `"…[…"` and
    /// reject legitimate documents.  This test specifically pins the
    /// escaped-quote path (`"\\""` must not toggle string state).
    #[test]
    fn check_record_bounds_ignores_brackets_inside_strings() {
        // Build a JSON object whose value is a string containing many
        // `[` characters — far more than MAX_RECORD_JSON_DEPTH.
        let inner = "[".repeat(MAX_RECORD_JSON_DEPTH * 4);
        let payload = format!(r#"{{"text":"{inner}","esc":"\""}}"#);
        // Sanity: the bracket count in the literal is well past the cap.
        assert!(inner.len() > MAX_RECORD_JSON_DEPTH);
        check_record_bounds(payload.as_bytes())
            .expect("string-internal brackets must not be counted toward nesting depth");
    }
}
