// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real AWS-backed [`KinesisSource`] adapter (`kinesis-io` feature).
//!
//! Thin: ALL consume / resume / decode logic lives feature-free behind
//! the [`KinesisSource`] trait, so this module only builds the
//! `aws-sdk-kinesis` client (default credential chain, explicit region,
//! optional endpoint override for LocalStack) and maps SDK errors onto
//! the crate's taxonomy — in particular `ExpiredIteratorException` onto
//! the distinct, matchable [`KinesisSourceError::ExpiredIterator`].
//!
//! TLS is the SDK's default rustls-based https client (no native-tls);
//! note the SDK's default crypto provider is aws-lc-rs, which needs a C
//! toolchain (cc/cmake) at BUILD time — same build-env posture as
//! `kafka-io`'s rdkafka cmake build, and the reason this feature is
//! default-OFF.

use aws_sdk_kinesis::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_kinesis::types::ShardIteratorType;

use crate::source::{
    GetRecordsOutput, KinesisRecord, KinesisSource, KinesisSourceError, ShardId, ShardIterator,
    StartPosition, StreamIdentity, classify_error_code,
};

/// [`KinesisSource`] over the official `aws-sdk-kinesis` client.
#[derive(Debug, Clone)]
pub struct AwsKinesisSource {
    client: aws_sdk_kinesis::Client,
}

impl AwsKinesisSource {
    /// Build a client from the DEFAULT AWS credential chain (env vars,
    /// shared profile, IMDS instance role — `aws-config`'s standard
    /// resolution), pinned to `region`, with an optional
    /// `endpoint_url` override (LocalStack: `http://localhost:4566`).
    ///
    /// STS assume-role (`awsAssumedRoleArn`) is NOT implemented in v1 —
    /// see [`KinesisIoConfig::log_unsupported_options`](crate::KinesisIoConfig::log_unsupported_options).
    pub async fn connect(region: &str, endpoint_url: Option<&str>) -> Self {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_owned()))
            .load()
            .await;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&sdk_config);
        if let Some(url) = endpoint_url {
            builder = builder.endpoint_url(url);
        }
        // Per-attempt operation deadline (Codex R5 H1): a hung endpoint
        // that accepts the connection but never responds must not block a
        // Kinesis RPC forever (which would freeze the consume loop and its
        // shutdown). This is the real-client defence-in-depth alongside the
        // loop-level `KINESIS_RPC_TIMEOUT` guard in the overlord.
        builder = builder.timeout_config(
            aws_sdk_kinesis::config::timeout::TimeoutConfig::builder()
                .operation_attempt_timeout(std::time::Duration::from_secs(30))
                .build(),
        );
        Self {
            client: aws_sdk_kinesis::Client::from_conf(builder.build()),
        }
    }

    /// Wrap an already-configured client (tests / bespoke config).
    #[must_use]
    pub fn from_client(client: aws_sdk_kinesis::Client) -> Self {
        Self { client }
    }
}

/// Map an SDK operation error onto the crate taxonomy via its error
/// code (see [`classify_error_code`] — pure and unit-tested without the
/// SDK).
fn map_sdk_error<E, R>(op: &str, err: &SdkError<E, R>) -> KinesisSourceError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    let code = err.code().map(str::to_owned);
    let service_error = matches!(err, SdkError::ServiceError(_));
    let detail = format!("{op}: {}", DisplayErrorContext(err));
    classify_error_code(code.as_deref(), service_error, detail)
}

#[async_trait::async_trait]
impl KinesisSource for AwsKinesisSource {
    async fn describe_stream(&self, stream: &str) -> Result<StreamIdentity, KinesisSourceError> {
        let out = self
            .client
            .describe_stream_summary()
            .stream_name(stream)
            .send()
            .await
            .map_err(|e| map_sdk_error("DescribeStreamSummary", &e))?;
        let Some(summary) = out.stream_description_summary() else {
            return Err(KinesisSourceError::Api(format!(
                "DescribeStreamSummary for {stream} returned no summary"
            )));
        };
        let creation_millis = summary
            .stream_creation_timestamp()
            .to_millis()
            .map_err(|e| {
                KinesisSourceError::Api(format!(
                    "DescribeStreamSummary for {stream}: unrepresentable \
                     StreamCreationTimestamp: {e}"
                ))
            })?;
        Ok(StreamIdentity {
            stream_arn: summary.stream_arn().to_owned(),
            stream_creation_timestamp_millis: creation_millis,
        })
    }

    async fn list_shards(&self, stream: &str) -> Result<Vec<ShardId>, KinesisSourceError> {
        let mut shards = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            // The API forbids StreamName together with NextToken: the
            // token alone scopes continuation pages.
            let req = match &next_token {
                None => self.client.list_shards().stream_name(stream),
                Some(token) => self.client.list_shards().next_token(token),
            };
            let out = req
                .send()
                .await
                .map_err(|e| map_sdk_error("ListShards", &e))?;
            shards.extend(
                out.shards()
                    .iter()
                    .map(|s| ShardId::new(s.shard_id().to_owned())),
            );
            match out.next_token() {
                Some(token) => next_token = Some(token.to_owned()),
                None => break,
            }
        }
        Ok(shards)
    }

    async fn get_shard_iterator(
        &self,
        stream: &str,
        shard: &ShardId,
        position: &StartPosition,
    ) -> Result<ShardIterator, KinesisSourceError> {
        let mut req = self
            .client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard.as_str());
        req = match position {
            StartPosition::TrimHorizon => req.shard_iterator_type(ShardIteratorType::TrimHorizon),
            StartPosition::Latest => req.shard_iterator_type(ShardIteratorType::Latest),
            StartPosition::AtSequenceNumber(seq) => req
                .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
                .starting_sequence_number(seq.as_str()),
            StartPosition::AfterSequenceNumber(seq) => req
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(seq.as_str()),
        };
        let out = req
            .send()
            .await
            .map_err(|e| map_sdk_error("GetShardIterator", &e))?;
        out.shard_iterator().map(ShardIterator::new).ok_or_else(|| {
            KinesisSourceError::Api(format!(
                "GetShardIterator for {stream}/{shard} returned no iterator"
            ))
        })
    }

    async fn get_records(
        &self,
        iterator: &ShardIterator,
    ) -> Result<GetRecordsOutput, KinesisSourceError> {
        let out = self
            .client
            .get_records()
            .shard_iterator(iterator.as_str())
            .send()
            .await
            .map_err(|e| map_sdk_error("GetRecords", &e))?;
        let records = out
            .records()
            .iter()
            .map(|r| KinesisRecord {
                partition_key: r.partition_key().to_owned(),
                sequence_number: r.sequence_number().to_owned(),
                data: r.data().as_ref().to_vec(),
                approximate_arrival_millis: r
                    .approximate_arrival_timestamp()
                    .and_then(|t| t.to_millis().ok()),
            })
            .collect();
        Ok(GetRecordsOutput {
            records,
            next_iterator: out.next_shard_iterator().map(ShardIterator::new),
            millis_behind_latest: out.millis_behind_latest(),
        })
    }
}
