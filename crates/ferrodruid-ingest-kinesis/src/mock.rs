// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Deterministic in-memory [`KinesisSource`] fake for tests.
//!
//! ALWAYS compiled and `pub` (not `#[cfg(test)]`) so OTHER crates'
//! tests — the overlord's supervisor integration tests in particular —
//! can construct and drive it without AWS, without docker, and without
//! the `kinesis-io` feature. It has no dependencies beyond std. Not
//! intended for production use (it is a test double; nothing stops you,
//! but it stores everything in memory forever).
//!
//! Deterministic behaviors the overlord tests script with it:
//!
//! * seed shards + records ([`MockKinesisSource::add_shard`],
//!   [`push_record`](MockKinesisSource::push_record) /
//!   [`push_json`](MockKinesisSource::push_json));
//! * expire every outstanding iterator
//!   ([`expire_iterators`](MockKinesisSource::expire_iterators)) — the
//!   next `get_records` on an old iterator fails with
//!   [`KinesisSourceError::ExpiredIterator`], exactly like the real
//!   5-minute TTL;
//! * simulate a stream delete+recreate
//!   ([`recreate_stream`](MockKinesisSource::recreate_stream)) — new
//!   creation timestamp (the durability generation marker), records
//!   wiped, outstanding iterators dead;
//! * paginate ([`set_records_per_call`](MockKinesisSource::set_records_per_call))
//!   and inject one-shot errors ([`push_error`](MockKinesisSource::push_error))
//!   to exercise retry paths;
//! * re-consume from any position — `get_shard_iterator` at
//!   TRIM_HORIZON / AT / AFTER replays the retained records, which is
//!   how zero-loss-across-restart is asserted without AWS.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::frontier::SeqNum;
use crate::source::{
    GetRecordsOutput, KinesisRecord, KinesisSource, KinesisSourceError, ShardId, ShardIterator,
    StartPosition, StreamIdentity,
};

/// Separator inside mock iterator tokens (shard ids never contain it).
const TOKEN_SEP: char = '|';

#[derive(Debug)]
struct MockShard {
    id: String,
    records: Vec<KinesisRecord>,
}

#[derive(Debug)]
struct MockState {
    stream: String,
    identity: StreamIdentity,
    shards: Vec<MockShard>,
    /// Bumped by [`MockKinesisSource::expire_iterators`] /
    /// [`MockKinesisSource::recreate_stream`]; a token minted under an
    /// older generation fails `get_records` with `ExpiredIterator`.
    generation: u64,
    records_per_call: usize,
    auto_seq: u64,
    scripted_errors: VecDeque<KinesisSourceError>,
    scripted_iterator_errors: VecDeque<KinesisSourceError>,
    get_records_calls: u64,
}

/// Deterministic in-memory [`KinesisSource`] implementation (see the
/// module docs). Cheap to clone — clones share the same underlying
/// stream state, so a test can keep a handle for seeding while the
/// consumer under test owns another.
#[derive(Debug, Clone)]
pub struct MockKinesisSource {
    inner: Arc<Mutex<MockState>>,
}

impl MockKinesisSource {
    /// Create a mock stream with a default identity (deterministic ARN,
    /// creation timestamp `1_700_000_000_000`).
    #[must_use]
    pub fn new(stream: impl Into<String>) -> Self {
        let stream = stream.into();
        let identity = StreamIdentity {
            stream_arn: format!("arn:aws:kinesis:us-east-1:000000000000:stream/{stream}"),
            stream_creation_timestamp_millis: 1_700_000_000_000,
        };
        Self {
            inner: Arc::new(Mutex::new(MockState {
                stream,
                identity,
                shards: Vec::new(),
                generation: 0,
                records_per_call: 10_000,
                auto_seq: 0,
                scripted_errors: VecDeque::new(),
                scripted_iterator_errors: VecDeque::new(),
                get_records_calls: 0,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, MockState> {
        // A poisoned lock only means a test thread panicked mid-call;
        // the state is plain data, safe to keep using.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Override the stream identity (ARN + creation timestamp).
    pub fn set_identity(&self, stream_arn: impl Into<String>, creation_millis: i64) {
        let mut st = self.lock();
        st.identity = StreamIdentity {
            stream_arn: stream_arn.into(),
            stream_creation_timestamp_millis: creation_millis,
        };
    }

    /// Add a shard (idempotent).
    pub fn add_shard(&self, shard_id: impl Into<String>) {
        let id = shard_id.into();
        let mut st = self.lock();
        if !st.shards.iter().any(|s| s.id == id) {
            st.shards.push(MockShard {
                id,
                records: Vec::new(),
            });
        }
    }

    /// Append a record with an EXPLICIT sequence number (must be a
    /// decimal digit string and increasing within the shard for the
    /// AT/AFTER positioning to behave like the real service — the mock
    /// does not enforce it). Creates the shard if absent.
    pub fn push_record(
        &self,
        shard_id: &str,
        partition_key: impl Into<String>,
        sequence_number: impl Into<String>,
        data: impl Into<Vec<u8>>,
        approximate_arrival_millis: Option<i64>,
    ) {
        let mut st = self.lock();
        let shard = ensure_shard(&mut st, shard_id);
        shard.records.push(KinesisRecord {
            partition_key: partition_key.into(),
            sequence_number: sequence_number.into(),
            data: data.into(),
            approximate_arrival_millis,
        });
    }

    /// Append a JSON row with an AUTO-ASSIGNED sequence number
    /// (56-digit, fixed-width, strictly increasing across the whole
    /// mock — realistic Kinesis shape). Returns the assigned sequence
    /// number. Creates the shard if absent.
    pub fn push_json(
        &self,
        shard_id: &str,
        partition_key: impl Into<String>,
        row: &serde_json::Value,
    ) -> String {
        let mut st = self.lock();
        st.auto_seq += 1;
        let seq = format!("4959{:052}", st.auto_seq);
        let shard = ensure_shard(&mut st, shard_id);
        shard.records.push(KinesisRecord {
            partition_key: partition_key.into(),
            sequence_number: seq.clone(),
            data: row.to_string().into_bytes(),
            approximate_arrival_millis: Some(1_700_000_000_000),
        });
        seq
    }

    /// Invalidate EVERY outstanding shard iterator: the next
    /// `get_records` with a previously-issued iterator fails with
    /// [`KinesisSourceError::ExpiredIterator`] (scripting the real
    /// 5-minute TTL). Freshly fetched iterators work normally.
    pub fn expire_iterators(&self) {
        self.lock().generation += 1;
    }

    /// Simulate a stream delete+recreate: all records wiped, all
    /// outstanding iterators dead, and the identity's creation
    /// timestamp replaced (the durability generation marker the
    /// frontier's recreation detection keys on). Shard ids are kept.
    pub fn recreate_stream(&self, new_creation_millis: i64) {
        let mut st = self.lock();
        st.generation += 1;
        st.identity.stream_creation_timestamp_millis = new_creation_millis;
        for shard in &mut st.shards {
            shard.records.clear();
        }
    }

    /// Cap how many records one `get_records` call returns (default
    /// 10 000, the real service limit) — lets tests force pagination.
    pub fn set_records_per_call(&self, n: usize) {
        self.lock().records_per_call = n.max(1);
    }

    /// Script a ONE-SHOT error: the next `get_records` call returns it
    /// instead of records (queued FIFO if called repeatedly).
    pub fn push_error(&self, err: KinesisSourceError) {
        self.lock().scripted_errors.push_back(err);
    }

    /// Script a ONE-SHOT error: the next `get_shard_iterator` call
    /// returns it instead of an iterator (queued FIFO if called
    /// repeatedly) — exercises the resume / re-seek error paths (the
    /// trim-fallback classification and the fatal StreamNotFound arm).
    pub fn push_iterator_error(&self, err: KinesisSourceError) {
        self.lock().scripted_iterator_errors.push_back(err);
    }

    /// How many `get_records` calls the mock has served (including
    /// scripted-error and expired-iterator outcomes).
    #[must_use]
    pub fn get_records_calls(&self) -> u64 {
        self.lock().get_records_calls
    }
}

fn ensure_shard<'a>(st: &'a mut MockState, shard_id: &str) -> &'a mut MockShard {
    if let Some(pos) = st.shards.iter().position(|s| s.id == shard_id) {
        return &mut st.shards[pos];
    }
    st.shards.push(MockShard {
        id: shard_id.to_owned(),
        records: Vec::new(),
    });
    let last = st.shards.len() - 1;
    &mut st.shards[last]
}

/// Position `records` per the requested start position: the index of
/// the first record to return. Records whose sequence number does not
/// parse as a decimal are treated as BELOW any requested position.
fn position_index(records: &[KinesisRecord], position: &StartPosition) -> usize {
    match position {
        StartPosition::TrimHorizon => 0,
        StartPosition::Latest => records.len(),
        StartPosition::AtSequenceNumber(target) => records
            .iter()
            .position(|r| SeqNum::parse(&r.sequence_number).is_ok_and(|s| s >= *target))
            .unwrap_or(records.len()),
        StartPosition::AfterSequenceNumber(target) => records
            .iter()
            .position(|r| SeqNum::parse(&r.sequence_number).is_ok_and(|s| s > *target))
            .unwrap_or(records.len()),
    }
}

fn make_token(shard_id: &str, index: usize, generation: u64) -> ShardIterator {
    ShardIterator::new(format!(
        "{shard_id}{TOKEN_SEP}{index}{TOKEN_SEP}{generation}"
    ))
}

fn parse_token(token: &ShardIterator) -> Result<(String, usize, u64), KinesisSourceError> {
    let raw = token.as_str();
    let mut parts = raw.rsplitn(3, TOKEN_SEP);
    let (Some(generation), Some(index), Some(shard)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(KinesisSourceError::Api(format!(
            "malformed mock iterator token: {raw:?}"
        )));
    };
    let (Ok(index), Ok(generation)) = (index.parse::<usize>(), generation.parse::<u64>()) else {
        return Err(KinesisSourceError::Api(format!(
            "malformed mock iterator token: {raw:?}"
        )));
    };
    Ok((shard.to_owned(), index, generation))
}

#[async_trait::async_trait]
impl KinesisSource for MockKinesisSource {
    async fn describe_stream(&self, stream: &str) -> Result<StreamIdentity, KinesisSourceError> {
        let st = self.lock();
        if st.stream != stream {
            return Err(KinesisSourceError::StreamNotFound(stream.to_owned()));
        }
        Ok(st.identity.clone())
    }

    async fn list_shards(&self, stream: &str) -> Result<Vec<ShardId>, KinesisSourceError> {
        let st = self.lock();
        if st.stream != stream {
            return Err(KinesisSourceError::StreamNotFound(stream.to_owned()));
        }
        Ok(st.shards.iter().map(|s| ShardId::new(&s.id)).collect())
    }

    async fn get_shard_iterator(
        &self,
        stream: &str,
        shard: &ShardId,
        position: &StartPosition,
    ) -> Result<ShardIterator, KinesisSourceError> {
        let mut st = self.lock();
        if let Some(err) = st.scripted_iterator_errors.pop_front() {
            return Err(err);
        }
        if st.stream != stream {
            return Err(KinesisSourceError::StreamNotFound(stream.to_owned()));
        }
        let Some(mock_shard) = st.shards.iter().find(|s| s.id == shard.as_str()) else {
            return Err(KinesisSourceError::Api(format!(
                "unknown shard {shard} in mock stream {stream}"
            )));
        };
        let index = position_index(&mock_shard.records, position);
        Ok(make_token(&mock_shard.id, index, st.generation))
    }

    async fn get_records(
        &self,
        iterator: &ShardIterator,
    ) -> Result<GetRecordsOutput, KinesisSourceError> {
        let mut st = self.lock();
        st.get_records_calls += 1;
        if let Some(err) = st.scripted_errors.pop_front() {
            return Err(err);
        }
        let (shard_id, index, generation) = parse_token(iterator)?;
        if generation != st.generation {
            return Err(KinesisSourceError::ExpiredIterator(format!(
                "mock iterator for shard {shard_id} expired (generation {generation} < {})",
                st.generation
            )));
        }
        let Some(mock_shard) = st.shards.iter().find(|s| s.id == shard_id) else {
            return Err(KinesisSourceError::Api(format!(
                "unknown shard {shard_id} in mock iterator"
            )));
        };
        let end = mock_shard
            .records
            .len()
            .min(index.saturating_add(st.records_per_call));
        let start = index.min(mock_shard.records.len());
        let records: Vec<KinesisRecord> = mock_shard.records[start..end].to_vec();
        let remaining = mock_shard.records.len() - end;
        // Deterministic proxy for `MillisBehindLatest`: 100ms per
        // unread record, 0 when caught up.
        let millis_behind = i64::try_from(remaining)
            .unwrap_or(i64::MAX)
            .saturating_mul(100);
        Ok(GetRecordsOutput {
            records,
            next_iterator: Some(make_token(&shard_id, end, st.generation)),
            millis_behind_latest: Some(millis_behind),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(s: &str) -> SeqNum {
        SeqNum::parse(s).expect("valid seq")
    }

    async fn drain(
        src: &MockKinesisSource,
        stream: &str,
        shard: &ShardId,
        position: &StartPosition,
    ) -> Vec<String> {
        let mut it = src
            .get_shard_iterator(stream, shard, position)
            .await
            .expect("iterator");
        let mut seqs = Vec::new();
        loop {
            let out = src.get_records(&it).await.expect("records");
            if out.records.is_empty() {
                assert_eq!(out.millis_behind_latest, Some(0));
                break;
            }
            seqs.extend(out.records.iter().map(|r| r.sequence_number.clone()));
            it = out.next_iterator.expect("open shard");
        }
        seqs
    }

    #[tokio::test]
    async fn trim_horizon_reads_all_in_order_with_pagination() {
        let src = MockKinesisSource::new("st");
        src.add_shard("shard-1");
        for i in 1..=5 {
            src.push_record("shard-1", "pk", format!("{i}00"), b"{}".to_vec(), None);
        }
        src.set_records_per_call(2); // force pagination
        let shard = ShardId::new("shard-1");
        let seqs = drain(&src, "st", &shard, &StartPosition::TrimHorizon).await;
        assert_eq!(seqs, vec!["100", "200", "300", "400", "500"]);
        // 2+2+1 record pages + 1 empty tail page.
        assert_eq!(src.get_records_calls(), 4);
    }

    #[tokio::test]
    async fn latest_sees_only_new_records() {
        let src = MockKinesisSource::new("st");
        src.push_record("s", "pk", "100", b"old".to_vec(), None);
        let shard = ShardId::new("s");
        let it = src
            .get_shard_iterator("st", &shard, &StartPosition::Latest)
            .await
            .expect("iterator");
        src.push_record("s", "pk", "200", b"new".to_vec(), None);
        let out = src.get_records(&it).await.expect("records");
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].sequence_number, "200");
        assert_eq!(out.records[0].data, b"new".to_vec());
    }

    #[tokio::test]
    async fn at_and_after_sequence_positions() {
        let src = MockKinesisSource::new("st");
        for s in ["100", "200", "300"] {
            src.push_record("s", "pk", s, b"{}".to_vec(), None);
        }
        let shard = ShardId::new("s");
        let at = drain(
            &src,
            "st",
            &shard,
            &StartPosition::AtSequenceNumber(seq("200")),
        )
        .await;
        assert_eq!(at, vec!["200", "300"]);
        let after = drain(
            &src,
            "st",
            &shard,
            &StartPosition::AfterSequenceNumber(seq("200")),
        )
        .await;
        assert_eq!(after, vec!["300"]);
        // AFTER the last record → nothing (caught up).
        let after_tip = drain(
            &src,
            "st",
            &shard,
            &StartPosition::AfterSequenceNumber(seq("300")),
        )
        .await;
        assert!(after_tip.is_empty());
    }

    #[tokio::test]
    async fn expired_iterator_then_reconsume_without_loss() {
        let src = MockKinesisSource::new("st");
        for s in ["100", "200", "300", "400"] {
            src.push_record("s", "pk", s, b"{}".to_vec(), None);
        }
        src.set_records_per_call(2);
        let shard = ShardId::new("s");
        let it = src
            .get_shard_iterator("st", &shard, &StartPosition::TrimHorizon)
            .await
            .expect("iterator");
        let out = src.get_records(&it).await.expect("first page");
        assert_eq!(out.records.len(), 2);
        let last_seen = seq(&out.records[1].sequence_number);
        let stale = out.next_iterator.expect("next");

        // Script the 5-minute TTL: the outstanding iterator dies.
        src.expire_iterators();
        let err = src.get_records(&stale).await.expect_err("expired");
        assert!(err.is_expired_iterator());

        // The consumer's mandatory recovery: re-get AFTER the last
        // consumed sequence and continue — no loss, no duplication.
        let rest = drain(
            &src,
            "st",
            &shard,
            &StartPosition::AfterSequenceNumber(last_seen),
        )
        .await;
        assert_eq!(rest, vec!["300", "400"]);
    }

    #[tokio::test]
    async fn describe_stream_and_recreation() {
        let src = MockKinesisSource::new("st");
        src.push_record("s", "pk", "100", b"{}".to_vec(), None);
        let id1 = src.describe_stream("st").await.expect("identity");
        assert_eq!(id1.stream_creation_timestamp_millis, 1_700_000_000_000);
        assert!(id1.stream_arn.ends_with("stream/st"));

        let shard = ShardId::new("s");
        let it = src
            .get_shard_iterator("st", &shard, &StartPosition::TrimHorizon)
            .await
            .expect("iterator");

        src.recreate_stream(1_800_000_000_000);
        // New generation marker...
        let id2 = src.describe_stream("st").await.expect("identity");
        assert_eq!(id2.stream_creation_timestamp_millis, 1_800_000_000_000);
        assert_eq!(id2.stream_arn, id1.stream_arn, "ARN reused on recreate");
        // ...old iterators dead, old records gone.
        assert!(
            src.get_records(&it)
                .await
                .expect_err("dead iterator")
                .is_expired_iterator()
        );
        let replay = drain(&src, "st", &shard, &StartPosition::TrimHorizon).await;
        assert!(replay.is_empty());
    }

    #[tokio::test]
    async fn unknown_stream_is_not_found() {
        let src = MockKinesisSource::new("st");
        src.add_shard("s");
        assert!(matches!(
            src.describe_stream("nope").await,
            Err(KinesisSourceError::StreamNotFound(_))
        ));
        assert!(matches!(
            src.list_shards("nope").await,
            Err(KinesisSourceError::StreamNotFound(_))
        ));
        assert!(matches!(
            src.get_shard_iterator("nope", &ShardId::new("s"), &StartPosition::TrimHorizon)
                .await,
            Err(KinesisSourceError::StreamNotFound(_))
        ));
    }

    #[tokio::test]
    async fn scripted_iterator_error_fires_once_then_recovers() {
        let src = MockKinesisSource::new("st");
        src.push_record("s", "pk", "100", b"{}".to_vec(), None);
        src.push_iterator_error(KinesisSourceError::Api("scripted".to_owned()));
        let shard = ShardId::new("s");
        assert!(matches!(
            src.get_shard_iterator("st", &shard, &StartPosition::TrimHorizon)
                .await,
            Err(KinesisSourceError::Api(_))
        ));
        // Same call retried → succeeds.
        let it = src
            .get_shard_iterator("st", &shard, &StartPosition::TrimHorizon)
            .await
            .expect("recovered");
        let out = src.get_records(&it).await.expect("records");
        assert_eq!(out.records.len(), 1);
    }

    #[tokio::test]
    async fn scripted_error_fires_once_then_recovers() {
        let src = MockKinesisSource::new("st");
        src.push_record("s", "pk", "100", b"{}".to_vec(), None);
        src.push_error(KinesisSourceError::Throttled("scripted".to_owned()));
        let shard = ShardId::new("s");
        let it = src
            .get_shard_iterator("st", &shard, &StartPosition::TrimHorizon)
            .await
            .expect("iterator");
        assert!(matches!(
            src.get_records(&it).await,
            Err(KinesisSourceError::Throttled(_))
        ));
        // Same iterator retried → succeeds.
        let out = src.get_records(&it).await.expect("recovered");
        assert_eq!(out.records.len(), 1);
    }
}
