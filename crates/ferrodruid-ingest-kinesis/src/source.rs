// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Transport abstraction over the Amazon Kinesis Data Streams API.
//!
//! [`KinesisSource`] is the seam that keeps the whole consume / resume /
//! publish pipeline unit-testable without AWS: the real adapter
//! ([`AwsKinesisSource`](crate::aws::AwsKinesisSource), behind the
//! `kinesis-io` feature) wraps `aws-sdk-kinesis`, while the deterministic
//! [`MockKinesisSource`](crate::mock::MockKinesisSource) drives the same
//! trait entirely in memory. The overlord's supervisor loop (next chunk)
//! is generic over this trait.
//!
//! Error taxonomy note: `ExpiredIteratorException` is modelled as the
//! DISTINCT, matchable [`KinesisSourceError::ExpiredIterator`] variant
//! because handling it is **mandatory**, not optional — Kinesis shard
//! iterators expire after 5 minutes, so any consumer that cannot detect
//! the expiry and re-call [`KinesisSource::get_shard_iterator`] silently
//! stalls forever on a slow or paused stream.

use crate::frontier::SeqNum;

/// A Kinesis shard id (e.g. `shardId-000000000000`). Opaque; ordered
/// lexicographically for deterministic iteration.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(String);

impl ShardId {
    /// Wrap a shard id string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The raw shard id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// An opaque shard-iterator token returned by
/// [`KinesisSource::get_shard_iterator`] / [`GetRecordsOutput`]. Valid
/// for at most 5 minutes on the real service; when it expires,
/// [`KinesisSource::get_records`] fails with
/// [`KinesisSourceError::ExpiredIterator`] and the caller must fetch a
/// fresh iterator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardIterator(String);

impl ShardIterator {
    /// Wrap an iterator token.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The raw iterator token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Where to start reading a shard.
///
/// `TrimHorizon` / `Latest` are the user-facing start positions selected
/// by the supervisor spec
/// ([`KinesisIoConfig::start_position`](crate::KinesisIoConfig::start_position));
/// `AtSequenceNumber` / `AfterSequenceNumber` are the INTERNAL resume
/// mechanism derived from the durable frontier
/// ([`ShardResume::resume_start_position`](crate::frontier::ShardResume::resume_start_position))
/// — they are not user-facing spec options in v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartPosition {
    /// Oldest retained record in the shard (Druid
    /// `useEarliestSequenceNumber=true`).
    TrimHorizon,
    /// Only records produced after the iterator is created (the Druid
    /// default).
    Latest,
    /// Start AT the given sequence number (inclusive) — re-consume from
    /// durable-evidence floor.
    AtSequenceNumber(SeqNum),
    /// Start AFTER the given sequence number (exclusive) — resume past
    /// contiguously-covered durable coverage.
    AfterSequenceNumber(SeqNum),
}

/// One record returned by [`KinesisSource::get_records`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KinesisRecord {
    /// The producer-chosen partition key.
    pub partition_key: String,
    /// The shard-unique sequence number — an OPAQUE decimal string
    /// (~56 digits on the real service; too large for `u128`/`i64`),
    /// ordered only within a shard. Parse with
    /// [`SeqNum::parse`](crate::frontier::SeqNum::parse) for comparisons.
    pub sequence_number: String,
    /// The record payload bytes (JSON per record in v1 — see
    /// [`decode_record`](crate::decode::decode_record)).
    pub data: Vec<u8>,
    /// Server-side approximate arrival timestamp, epoch millis.
    pub approximate_arrival_millis: Option<i64>,
}

/// The result of one [`KinesisSource::get_records`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRecordsOutput {
    /// Records read, in shard order. May be empty while the iterator is
    /// still advancing (Kinesis returns empty pages routinely).
    pub records: Vec<KinesisRecord>,
    /// The iterator to use for the NEXT call; `None` means the shard is
    /// closed and fully consumed (resharding — unsupported in v1, the
    /// caller should stop polling the shard and warn).
    pub next_iterator: Option<ShardIterator>,
    /// How far this iterator is behind the tip of the shard, in millis.
    /// `0` means caught up; `None` if the service did not report it.
    pub millis_behind_latest: Option<i64>,
}

/// Stream identity used for delete/recreate detection.
///
/// The ARN alone CANNOT detect a same-name delete+recreate (the ARN is
/// reused in the same account/region), so the creation timestamp is the
/// authoritative generation marker — the analogue of Kafka's KIP-516
/// topic id in the durability model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamIdentity {
    /// The stream ARN.
    pub stream_arn: String,
    /// Stream creation time, epoch millis — the generation marker
    /// stamped into durable segment payloads and required to match
    /// before durable coverage may advance a resume.
    pub stream_creation_timestamp_millis: i64,
}

/// Errors from a [`KinesisSource`].
#[derive(Debug, thiserror::Error)]
pub enum KinesisSourceError {
    /// The shard iterator passed to [`KinesisSource::get_records`] has
    /// expired (5-minute TTL on the real service). NOT fatal: the caller
    /// MUST re-call [`KinesisSource::get_shard_iterator`] and continue.
    #[error("kinesis shard iterator expired (re-get the iterator): {0}")]
    ExpiredIterator(String),
    /// The stream (or shard) does not exist.
    #[error("kinesis stream not found: {0}")]
    StreamNotFound(String),
    /// Read throughput exceeded / throttled — retryable with backoff.
    #[error("kinesis throughput exceeded (throttled): {0}")]
    Throttled(String),
    /// Authentication / authorization failure (credentials, signature,
    /// access denied) — fatal until the operator fixes credentials.
    #[error("kinesis auth failure: {0}")]
    Auth(String),
    /// Transport-level failure (connect, timeout, TLS, response decode).
    #[error("kinesis transport error: {0}")]
    Transport(String),
    /// The request was REJECTED as invalid by the service
    /// (`InvalidArgumentException` / `SequenceNumberOutOfRangeException`).
    /// On a `GetShardIterator` resume seek this is the
    /// retention-trimmed / out-of-range sequence case — the ONLY error
    /// class for which the consumer's TRIM_HORIZON fallback is sound.
    /// Transient service failures classify as [`Api`](Self::Api) and
    /// must be RETRIED at the same position, never downgraded to a
    /// TRIM_HORIZON reset (which re-consumes the whole retention window
    /// into append-only segments: permanent double-counting).
    #[error("kinesis invalid argument (rejected request): {0}")]
    InvalidArgument(String),
    /// Any other service-side API error.
    #[error("kinesis api error: {0}")]
    Api(String),
}

impl KinesisSourceError {
    /// `true` for the (mandatory-to-handle) expired-iterator case, so
    /// call sites can match without destructuring.
    #[must_use]
    pub fn is_expired_iterator(&self) -> bool {
        matches!(self, Self::ExpiredIterator(_))
    }
}

/// Classify an AWS error code string onto the [`KinesisSourceError`]
/// taxonomy. Pure so the mapping is unit-testable without the SDK; the
/// `kinesis-io` adapter feeds it the code from the SDK's error metadata.
///
/// `service_error` distinguishes a service response that carried no
/// recognizable code (→ [`KinesisSourceError::Api`]) from a failure that
/// never got a service response (→ [`KinesisSourceError::Transport`]).
#[must_use]
pub fn classify_error_code(
    code: Option<&str>,
    service_error: bool,
    detail: String,
) -> KinesisSourceError {
    match code {
        Some("ExpiredIteratorException") => KinesisSourceError::ExpiredIterator(detail),
        Some(
            "ProvisionedThroughputExceededException"
            | "LimitExceededException"
            | "ThrottlingException",
        ) => KinesisSourceError::Throttled(detail),
        Some("ResourceNotFoundException") => KinesisSourceError::StreamNotFound(detail),
        Some(
            "AccessDeniedException"
            | "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "ExpiredTokenException"
            | "MissingAuthenticationTokenException"
            | "IncompleteSignatureException",
        ) => KinesisSourceError::Auth(detail),
        // The service REJECTED the request as invalid — distinct from a
        // generic/transient Api failure because it is the only class the
        // resume path may answer with a TRIM_HORIZON fallback (a
        // retention-trimmed sequence raises `InvalidArgumentException`
        // on the real service).
        Some("InvalidArgumentException" | "SequenceNumberOutOfRangeException") => {
            KinesisSourceError::InvalidArgument(detail)
        }
        Some(_) => KinesisSourceError::Api(detail),
        None if service_error => KinesisSourceError::Api(detail),
        None => KinesisSourceError::Transport(detail),
    }
}

/// Transport-abstracted Kinesis client (classic polling `GetRecords`
/// model; enhanced fan-out / `SubscribeToShard` is out of scope in v1).
///
/// Object-safe (`async_trait`) so the overlord can hold a
/// `Box<dyn KinesisSource>` per supervisor.
#[async_trait::async_trait]
pub trait KinesisSource: Send + Sync {
    /// Fetch the stream's identity (ARN + creation timestamp). Called at
    /// consumer start; the creation timestamp is stamped into durable
    /// segment payloads as the generation marker (see
    /// [`StreamIdentity`]).
    async fn describe_stream(&self, stream: &str) -> Result<StreamIdentity, KinesisSourceError>;

    /// List the stream's shard ids (paginated internally). v1 snapshots
    /// the shard set once at consumer start — resharding (split/merge)
    /// is unsupported and requires a supervisor restart.
    async fn list_shards(&self, stream: &str) -> Result<Vec<ShardId>, KinesisSourceError>;

    /// Obtain a shard iterator at `position`. Iterators expire after 5
    /// minutes on the real service — see
    /// [`KinesisSourceError::ExpiredIterator`].
    async fn get_shard_iterator(
        &self,
        stream: &str,
        shard: &ShardId,
        position: &StartPosition,
    ) -> Result<ShardIterator, KinesisSourceError>;

    /// Read the next batch of records. On
    /// [`KinesisSourceError::ExpiredIterator`] the caller must re-call
    /// [`Self::get_shard_iterator`] (resuming AFTER the last sequence
    /// number it consumed) and retry.
    async fn get_records(
        &self,
        iterator: &ShardIterator,
    ) -> Result<GetRecordsOutput, KinesisSourceError>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_iterator_is_distinct_and_matchable() {
        let err = KinesisSourceError::ExpiredIterator("token".to_owned());
        assert!(err.is_expired_iterator());
        assert!(!KinesisSourceError::Api("x".to_owned()).is_expired_iterator());
        assert!(!KinesisSourceError::Throttled("x".to_owned()).is_expired_iterator());
    }

    #[test]
    fn classify_error_code_taxonomy() {
        let detail = || "d".to_owned();
        assert!(matches!(
            classify_error_code(Some("ExpiredIteratorException"), true, detail()),
            KinesisSourceError::ExpiredIterator(_)
        ));
        assert!(matches!(
            classify_error_code(
                Some("ProvisionedThroughputExceededException"),
                true,
                detail()
            ),
            KinesisSourceError::Throttled(_)
        ));
        assert!(matches!(
            classify_error_code(Some("LimitExceededException"), true, detail()),
            KinesisSourceError::Throttled(_)
        ));
        assert!(matches!(
            classify_error_code(Some("ResourceNotFoundException"), true, detail()),
            KinesisSourceError::StreamNotFound(_)
        ));
        assert!(matches!(
            classify_error_code(Some("AccessDeniedException"), true, detail()),
            KinesisSourceError::Auth(_)
        ));
        assert!(matches!(
            classify_error_code(Some("UnrecognizedClientException"), true, detail()),
            KinesisSourceError::Auth(_)
        ));
        // The invalid-position rejection is DISTINCT from Api: only it
        // may trigger the resume path's TRIM_HORIZON fallback.
        assert!(matches!(
            classify_error_code(Some("InvalidArgumentException"), true, detail()),
            KinesisSourceError::InvalidArgument(_)
        ));
        assert!(matches!(
            classify_error_code(Some("SequenceNumberOutOfRangeException"), true, detail()),
            KinesisSourceError::InvalidArgument(_)
        ));
        // Unknown code from the service → Api; no code + no service
        // response → Transport; no code but a service response → Api.
        assert!(matches!(
            classify_error_code(Some("SomethingNew"), true, detail()),
            KinesisSourceError::Api(_)
        ));
        assert!(matches!(
            classify_error_code(None, false, detail()),
            KinesisSourceError::Transport(_)
        ));
        assert!(matches!(
            classify_error_code(None, true, detail()),
            KinesisSourceError::Api(_)
        ));
    }
}
