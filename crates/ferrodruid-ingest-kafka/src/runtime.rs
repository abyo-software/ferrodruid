// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kafka supervisor runtime that manages multiple consumer tasks.

use std::collections::HashMap;

use ferrodruid_ingest_batch::{DimensionSchema, DimensionType};
use tokio::sync::mpsc;

use crate::consumer::KafkaConsumerConfig;
use crate::{DimensionEntry, KafkaSupervisorSpec};

/// Kafka consumer-property keys the runtime manages itself; any
/// caller-supplied value for these in `consumerProperties` is dropped so
/// it cannot override correctness-critical behaviour (offset commit is
/// kept OFF; the group id and reset policy are derived from the spec; the
/// prefetch-memory limits are pinned conservatively in
/// `create_stream_consumer` so a spec cannot make librdkafka buffer GiBs
/// of records ahead of the application byte cap — Codex R4; the rebalance
/// protocol is pinned to `cooperative-sticky` there too — Codex R29).
/// `max.poll.interval.ms` is intentionally NOT in this list: it is forwarded
/// and FLOORED to `max(caller, 300000)` in `create_stream_consumer` so a spec
/// cannot go below the safe default yet a legitimately larger value survives
/// — Codex R36.
///
/// # Consumer-property hardening policy (Codex R37 — closing the CLASS)
///
/// `consumerProperties` are forwarded to librdkafka, so an operator can set a
/// *data-affecting* property — one that changes WHICH records are selected,
/// their INTEGRITY, partition/topic DISCOVERY, cluster IDENTITY, or
/// at-least-once ordering — in a way that silently drops / mis-values /
/// mis-attributes acknowledged rows. Instead of patching one property per
/// round, every forwarded consumer property in librdkafka 2.12.1 (rdkafka-sys
/// 4.10.0+2.12.1 `librdkafka/CONFIGURATION.md`) was swept and given ONE
/// disposition. Rule: **pin** what a fixed value makes harmless (re-set last,
/// here + `build_client_config`); **reject** what breaks semantics a pin
/// cannot express (`validate`-time `Err`); **fingerprint** a record-selecting
/// but legitimate knob (reflected in `schema_fingerprint` so a change refuses
/// a lossy re-create); **allow** the data-neutral operational / connection
/// tuning real secured clusters need.
///
/// | Property | Disposition | Why |
/// |---|---|---|
/// | `bootstrap.servers` | pin (from spec) | the source cluster is the spec's, not an override |
/// | `group.id` | pin (stable `ferrodruid-{id}`) | committed offsets must be consulted across restarts to resume past the durable frontier (compat-3 stage 2) |
/// | `enable.auto.commit` | pin `false` | committing past memory-resident data loses acked rows on restart |
/// | `auto.offset.reset` | pin (from `useEarliestOffset`) | the replay start point is a spec decision |
/// | `partition.assignment.strategy` / `group.protocol` | pin | an eager assignor re-delivers every partition on any rebalance (R29) |
/// | `max.poll.interval.ms` | floor (`>= 300000`, `<= 86400000`) | a tiny value self-revokes on a slow publish → in-session re-delivery (R36) |
/// | `metadata.recovery.strategy` | pin `none` | a re-bootstrap can migrate the live session to a different cluster (R30 F1) |
/// | `check.crcs` | **pin `true`** | default `false` lets a bit-flipped record parse as a WRONG value (R37 F1) |
/// | `topic.metadata.refresh.interval.ms` | **clamp `[floor,300000]`** | `-1` disables new-partition discovery → records never ingested; but a faster caller value is kept (R37 F2) |
/// | `allow.auto.create.topics` | **pin `false`** | `true` ingests from an auto-created EMPTY topic instead of failing on a typo |
/// | `group.instance.id` | **drop** (absence ⇒ dynamic) | static membership makes `FencedInstanceId` reachable → a silently-stopped consumer |
/// | `queued.*` / `fetch.*` / `*.message.*bytes` | pin/drop | bound librdkafka's prefetch below the app byte cap (R4/R7) |
/// | `topic.blacklist` | reject (`validate`) | a pattern matching the one literal topic blanks the subscription → empty replay = loss (R29) |
/// | `topic` `^…` / `topicPattern` | reject (`validate`) | librdkafka regex-subscribes → multi-topic ingest breaks pair/provenance (R28) |
/// | `isolation.level` | fingerprint (R27) | selects committed vs +aborted records — legitimate, but a change refuses a lossy re-create |
/// | `api.version.request` | **pin `true`** | `false` (+ a pre-0.11 `broker.version.fallback`, or a forced negotiation timeout) makes librdkafka speak a pre-v4 Fetch a down-converting 2.x/3.x broker serves as READ_UNCOMMITTED → aborted transactional rows bypass `read_committed` (R38, KIP-98) |
/// | `broker.version.fallback` | **pin `0.11.0.0`** | the feature set assumed when ApiVersion negotiation is disabled OR fails; floored to the first transactional-Fetch (KIP-98) release so even a forced fallback keeps `isolation.level` effective (a value >= 0.10 also independently re-enables ApiVersionRequests) (R38) |
/// | `api.version.fallback.ms` | **drop** | deprecated knob for how long the (now-safe) fallback is used; no legitimate use here, dropped so its absence ⇒ the default 0 (R38) |
/// | `api.version.request.timeout.ms` | allow | only the negotiation-request timeout; the pinned `broker.version.fallback` makes even a timed-out negotiation fall back to a `read_committed`-capable feature set, so it cannot downgrade the protocol (R38) |
/// | `session.timeout.ms` | allow (+ fatal-classify) | liveness tuning; a broker-rejected value fails LOUD via `InvalidSessionTimeout` (R35) |
/// | `heartbeat.interval.ms` / `metadata.max.age.ms` / `topic.metadata.refresh.fast.interval.ms` | allow | liveness / cache-age tuning; discovery is driven by the clamped refresh interval |
/// | `client.dns.lookup` | allow | resolution mode only; cluster IDENTITY is enforced by cluster-id (R30 F1 / R37 F3), not DNS |
/// | `security.protocol` / `sasl.*` / `ssl.*` (≠ `ssl.engine.*`) / `socket.*` / `connections.max.idle.ms` | allow | connection + credential tuning secured clusters require |
/// | `plugin.library.paths` / `ssl.engine.*` | drop ([`FORBIDDEN_CONSUMER_KEYS`]) | dlopen native code under the server's privileges |
///
/// Not present in librdkafka (documented for completeness): there is **no**
/// `topic.whitelist` consumer property, and `partition.assignment.strategy`
/// is the only `partition.assignment.*` key. For a CONSUMER within-partition
/// delivery is always offset-ordered, so no property reorders delivered
/// records (nothing to pin for "ordering").
const MANAGED_CONSUMER_KEYS: &[&str] = &[
    "bootstrap.servers",
    // `metadata.broker.list` is librdkafka's ALIAS for `bootstrap.servers`
    // (same underlying property). It must be dropped too (Codex R37 round-5):
    // `build_client_config` stores caller props and the pinned
    // `bootstrap.servers` under DISTINCT `ClientConfig` HashMap keys, and
    // rdkafka applies them to librdkafka in non-deterministic map order — so a
    // surviving `metadata.broker.list` could be applied AFTER the pin and win,
    // silently pointing the consumer at a DIFFERENT cluster than the spec's.
    "metadata.broker.list",
    "group.id",
    "enable.auto.commit",
    "auto.offset.reset",
    // Rebalance protocol (Codex R29): `create_stream_consumer` pins
    // `partition.assignment.strategy=cooperative-sticky` — an eager
    // (range/roundrobin) override would let ANY rebalance (e.g. a
    // partition-count increase) revoke every owned partition and discard the
    // in-memory fetch positions, re-consuming each from the last committed
    // offset (compat-3 stage 2) — a partition whose most recent records were
    // consumed but not yet committed (no roll since) is then re-delivered
    // in-process (bounded at-least-once duplication). Cooperative-sticky
    // avoids revoking unaffected partitions entirely.
    // `group.protocol` is managed alongside it: under `consumer` (KIP-848)
    // librdkafka REJECTS a set `partition.assignment.strategy` at client
    // creation, so a spec could otherwise turn the pin into a supervisor
    // that never starts; the runtime runs the classic protocol.
    "partition.assignment.strategy",
    "group.protocol",
    // NOTE: `max.poll.interval.ms` is deliberately NOT managed/stripped here.
    // It is FORWARDED so `create_stream_consumer` can FLOOR it to
    // `max(caller, 300000)` (Codex R36): a floor blocks a pathologically small
    // caller value (the finding's `1`, which would let a mid-publish block
    // revoke every partition and force a duplicating earliest re-consume)
    // while PRESERVING a legitimately larger caller value — a spec may need
    // `max.poll.interval.ms >= session.timeout.ms`, which librdkafka requires,
    // so stripping/exact-pinning would break valid large-session configs.
    // Prefetch-memory limits (pinned, not caller-tunable) — incl. the
    // per-partition / protocol aliases that can otherwise raise the effective
    // fetch size past the pinned cap (Codex R7).
    "queued.max.messages.kbytes",
    "queued.min.messages",
    "fetch.max.bytes",
    "fetch.message.max.bytes",
    "max.partition.fetch.bytes",
    "message.max.bytes",
    "receive.message.max.bytes",
    // --- Codex R37: close the "operator sets a data-affecting property" class.
    // These are exact-PINNED (not just dropped) in `create_stream_consumer`;
    // dropping here removes the caller's value so the pin (applied LAST) wins.
    //
    // Payload INTEGRITY (R37 F1): `check.crcs` defaults to `false`, so a
    // bit-flipped record that still parses as valid JSON is published with a
    // WRONG value. Pinned `true` so every consumed record is CRC-verified; a
    // detected corruption then surfaces as `BadMessage`, which
    // `consume_error_is_fatal` treats as replay-required (no silent skip).
    "check.crcs",
    // Topic completeness: `allow.auto.create.topics=true` makes a subscribe to
    // a MISSING topic auto-create an EMPTY one (when the broker permits),
    // masking a topic typo and ingesting from an empty log instead of failing
    // — after an earliest cleanup the empty replay rebuilds nothing. Pinned
    // `false` (also the librdkafka consumer default).
    "allow.auto.create.topics",
    // Group IDENTITY / rebalance: `group.instance.id` enables STATIC
    // membership, which makes `FencedInstanceId` reachable — two members with
    // one instance id fence each other and one silently stops consuming (after
    // an earliest cleanup that is permanent loss). It also serves no purpose
    // with this crate's unique-per-spawn `group.id`. Dropped: its ABSENCE is
    // the correct default (dynamic membership), so no pin value is needed, and
    // dropping keeps the `FencedInstanceId`-is-unreachable invariant that
    // `consume_error_is_fatal` documents.
    "group.instance.id",
    // --- Codex R38: close the protocol-downgrade sub-class of "an operator
    // sets a data-affecting property". `isolation.level=read_committed` (R27,
    // the pinned librdkafka default) only takes effect from Fetch **v4**
    // (KIP-98, Kafka 0.11.0.0); an older Fetch carries no isolation flag and a
    // down-conversion-enabled 2.x/3.x broker serves it as READ_UNCOMMITTED,
    // silently delivering ABORTED transactional records that then publish as
    // valid rows — under a schemaFp stamped `read_committed`. librdkafka picks
    // the Fetch version from ApiVersion NEGOTIATION (or, when that is disabled
    // or fails, from `broker.version.fallback`), so three deprecated pre-0.10
    // knobs can force the downgrade. None has a legitimate use here (we only
    // support modern transactional brokers), so all three are runtime-managed:
    //
    //   * `api.version.request` — dropped; `build_client_config` pins it `true`
    //     so the broker's REAL feature set is always negotiated (a modern
    //     broker then advertises Fetch v4+ and `read_committed` is honoured).
    //   * `broker.version.fallback` — dropped; pinned to `0.11.0.0` (the first
    //     transactional-Fetch release). It is only consulted when negotiation
    //     is disabled OR fails, so pinning a read_committed-capable version
    //     closes the residual "negotiation genuinely fails / an
    //     `api.version.request.timeout.ms=1` forces a timeout" path too. Per
    //     CONFIGURATION.md a value >= 0.10 also re-enables ApiVersionRequests.
    //   * `api.version.fallback.ms` — dropped (its absence ⇒ the default 0).
    //     It only governs how long the now-safe fallback is used, so there is
    //     no value to pin; drop-only, like `group.instance.id`.
    //
    // `api.version.request.timeout.ms` is NOT managed (allowed): it is only the
    // negotiation-request timeout, and the `0.11.0.0` fallback pin makes even a
    // timed-out negotiation degrade to a read_committed-capable feature set —
    // it cannot select the protocol version. The CONFIGURATION.md sweep found
    // no other property that fixes the Fetch/protocol version (librdkafka has
    // no `fetch.version`-style knob), so this closes the sub-class.
    "api.version.request",
    "broker.version.fallback",
    "api.version.fallback.ms",
    // NOTE: `topic.metadata.refresh.interval.ms` (R37 F2) is deliberately NOT
    // in this list. It is FORWARDED so `create_stream_consumer` can CLAMP it to
    // `[floor, 300000]` (like `max.poll.interval.ms`): `-1`/`0`/invalid — which
    // would DISABLE new-partition discovery — become the safe default, and a
    // value SLOWER than the default is capped, but a legitimately FASTER caller
    // value (needed for short-retention topics that add partitions) is PRESERVED.
    // Exact-pinning it would override that faster setting and re-open the very
    // discovery-lag loss the finding closes.
];

/// Consumer-property keys that must NEVER be forwarded to librdkafka
/// because they cause it to load native code / arbitrary files under the
/// server's privileges. A supervisor spec is a semi-trusted, datasource-
/// writer-supplied document, so these are dropped (defence-in-depth):
///
/// * `plugin.library.paths` — dlopen()s arbitrary `.so` interceptors,
/// * `ssl.engine.location` / `ssl.engine.id` — load an OpenSSL ENGINE
///   (native code) from an attacker-chosen path.
///
/// Ordinary SASL/SSL connection + credential properties (`security.protocol`,
/// `sasl.*`, `ssl.ca.location`, …) are still forwarded so real secured
/// clusters work.
const FORBIDDEN_CONSUMER_KEYS: &[&str] = &[
    "plugin.library.paths",
    "ssl.engine.location",
    "ssl.engine.id",
];

/// Map a Druid `dimensionsSpec.dimensions` entry to a typed
/// [`DimensionSchema`], preserving `long` / `float` / `double` types
/// (unknown / absent types default to `string`, matching Druid).
fn dimension_entry_to_schema(entry: &DimensionEntry) -> DimensionSchema {
    match entry {
        DimensionEntry::String(name) => DimensionSchema::string(name.clone()),
        DimensionEntry::Typed { name, dim_type } => {
            let dt = match dim_type.as_str() {
                "long" => DimensionType::Long,
                "float" => DimensionType::Float,
                "double" => DimensionType::Double,
                _ => DimensionType::String,
            };
            DimensionSchema::new(name.clone(), dt)
        }
    }
}

/// Runtime that manages multiple [`KafkaConsumerTask`] instances for a supervisor.
pub struct KafkaSupervisorRuntime {
    /// Unique supervisor identifier.
    pub supervisor_id: String,
    /// The supervisor specification.
    pub spec: KafkaSupervisorSpec,
    shutdown_txs: Vec<mpsc::Sender<()>>,
    task_count: usize,
    running: bool,
}

impl KafkaSupervisorRuntime {
    /// Create a new supervisor runtime (not yet started).
    pub fn new(supervisor_id: String, spec: KafkaSupervisorSpec) -> Self {
        Self {
            supervisor_id,
            spec,
            shutdown_txs: Vec::new(),
            task_count: 0,
            running: false,
        }
    }

    /// Build a [`KafkaConsumerConfig`] from the supervisor spec.
    pub fn build_consumer_config(&self) -> KafkaConsumerConfig {
        let brokers = self
            .spec
            .io_config
            .consumer_properties
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());

        let dim_schemas: Vec<DimensionSchema> = self
            .spec
            .data_schema
            .dimensions_spec
            .dimensions
            .iter()
            .map(dimension_entry_to_schema)
            .collect();

        let max_rows = self
            .spec
            .tuning_config
            .as_ref()
            .and_then(|t| t.max_rows_per_segment)
            .unwrap_or(5_000_000);

        // Forward every caller-supplied consumer property (so
        // `security.protocol`, SASL/SSL credentials, etc. reach librdkafka
        // and a supervisor for a secured Kafka cluster actually connects),
        // EXCEPT the keys the runtime manages itself
        // ([`MANAGED_CONSUMER_KEYS`]) and the native-code-loading keys that
        // must never be honoured from a spec ([`FORBIDDEN_CONSUMER_KEYS`]).
        let additional_properties: HashMap<String, String> = self
            .spec
            .io_config
            .consumer_properties
            .iter()
            .filter(|(k, _)| {
                let k = k.as_str();
                if FORBIDDEN_CONSUMER_KEYS.contains(&k) {
                    tracing::warn!(
                        supervisor_id = %self.supervisor_id,
                        key = %k,
                        "dropping forbidden Kafka consumer property (native-code loading)",
                    );
                    return false;
                }
                !MANAGED_CONSUMER_KEYS.contains(&k)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // STABLE group id (compat-3 stage 2). Segments are now durable (deep
        // storage) and the consumer MANUAL-commits the per-partition offset
        // after each successful, persisted publish (`enable.auto.commit=false`
        // kept). A stable `ferrodruid-{supervisor_id}` — derived from the
        // datasource-stable supervisor id, so it is the SAME across restarts —
        // is what lets Kafka return that committed offset on resume, so the
        // consumer continues past the durable frontier instead of replaying
        // the whole topic (the pre-stage-2 nanosecond-unique group forced a
        // full earliest replay every start). `auto.offset.reset` /
        // `useEarliestOffset` now govern only a FIRST start with no committed
        // offset — exactly Druid's behaviour. Belt-and-suspenders against a
        // stale/lagging committed value: the overlord additionally seeks each
        // partition past the frontier derived from the durable segments'
        // `payload.kafkaOffsets`.

        // Thread the VALIDATED format through (validate() restricts it to
        // auto|iso|millis) — the formats genuinely differ at extraction time
        // (a declared `iso` reads "2023" as the YEAR 2023 — Fable audit).
        let timestamp_format = match self
            .spec
            .data_schema
            .timestamp_spec
            .format
            .to_ascii_lowercase()
            .as_str()
        {
            "iso" => ferrodruid_ingest_batch::TsFormat::Iso,
            "millis" => ferrodruid_ingest_batch::TsFormat::Millis,
            _ => ferrodruid_ingest_batch::TsFormat::Auto,
        };

        KafkaConsumerConfig {
            brokers,
            topic: self.spec.io_config.topic.clone(),
            group_id: format!("ferrodruid-{}", self.supervisor_id),
            data_source: self.spec.data_schema.data_source.clone(),
            timestamp_column: self.spec.data_schema.timestamp_spec.column.clone(),
            timestamp_format,
            dim_schemas,
            metrics_specs: self.spec.data_schema.metrics_spec.clone(),
            max_rows_per_segment: max_rows,
            segment_flush_interval_ms: 10_000,
            use_earliest_offset: self.spec.io_config.use_earliest_offset.unwrap_or(false),
            additional_properties,
            output_dir: None,
        }
    }

    /// Start the supervisor by creating consumer tasks.
    ///
    /// Each task gets its own shutdown channel. In a real implementation with the
    /// `kafka-io` feature, each task would be spawned as a tokio task. Without it,
    /// this just records the task count and marks the runtime as running.
    pub fn start(&mut self, task_count: usize) {
        self.task_count = task_count;
        self.shutdown_txs.clear();

        for _ in 0..task_count {
            let (tx, _rx) = mpsc::channel(1);
            self.shutdown_txs.push(tx);
        }
        self.running = true;
    }

    /// Stop all consumer tasks gracefully.
    pub async fn stop(&mut self) {
        for tx in self.shutdown_txs.drain(..) {
            let _ = tx.send(()).await;
        }
        self.running = false;
        self.task_count = 0;
    }

    /// Get the number of running tasks.
    pub fn running_task_count(&self) -> usize {
        self.task_count
    }

    /// Whether the supervisor runtime is running.
    pub fn is_running(&self) -> bool {
        self.running
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataSchema, DimensionEntry, DimensionsSpec, KafkaIoConfig, KafkaSupervisorSpec,
        KafkaTuningConfig, TimestampSpec,
    };

    fn sample_spec() -> KafkaSupervisorSpec {
        KafkaSupervisorSpec {
            spec_type: "kafka".to_string(),
            data_schema: DataSchema {
                data_source: "events".to_string(),
                timestamp_spec: TimestampSpec {
                    column: "__time".to_string(),
                    format: "auto".to_string(),
                },
                dimensions_spec: DimensionsSpec {
                    dimensions: vec![
                        DimensionEntry::String("page".to_string()),
                        DimensionEntry::Typed {
                            name: "user".to_string(),
                            dim_type: "string".to_string(),
                        },
                    ],
                    dimension_exclusions: vec![],
                    use_schema_discovery: None,
                },
                metrics_spec: vec![],
                granularity_spec: None,
                transform_spec: None,
            },
            io_config: KafkaIoConfig {
                topic: "wiki-events".to_string(),
                topic_pattern: None,
                consumer_properties: {
                    let mut m = HashMap::new();
                    m.insert("bootstrap.servers".to_string(), "kafka:9092".to_string());
                    m
                },
                input_format: None,
                task_count: Some(3),
                replicas: Some(1),
                task_duration: None,
                use_earliest_offset: Some(true),
            },
            tuning_config: Some(KafkaTuningConfig {
                max_rows_in_memory: Some(75_000),
                max_rows_per_segment: Some(1_000_000),
                max_total_rows: None,
                intermediate_persist_period: None,
            }),
        }
    }

    #[test]
    fn runtime_build_config() {
        let runtime = KafkaSupervisorRuntime::new("test-sup".to_string(), sample_spec());
        let config = runtime.build_consumer_config();

        assert_eq!(config.brokers, "kafka:9092");
        assert_eq!(config.topic, "wiki-events");
        // Stable group id (compat-3 stage 2): exactly `ferrodruid-{id}`, no
        // per-spawn token, so committed offsets are consulted across restarts.
        assert_eq!(
            config.group_id, "ferrodruid-test-sup",
            "group_id = {}",
            config.group_id
        );
        assert_eq!(config.data_source, "events");
        assert_eq!(config.timestamp_column, "__time");
        let names: Vec<&str> = config.dim_schemas.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["page", "user"]);
        assert_eq!(config.max_rows_per_segment, 1_000_000);
        assert!(config.use_earliest_offset);
    }

    #[test]
    fn typed_dimensions_preserve_declared_types() {
        use ferrodruid_ingest_batch::DimensionType;
        let mut spec = sample_spec();
        spec.data_schema.dimensions_spec.dimensions = vec![
            DimensionEntry::String("page".to_string()),
            DimensionEntry::Typed {
                name: "count".to_string(),
                dim_type: "long".to_string(),
            },
            DimensionEntry::Typed {
                name: "ratio".to_string(),
                dim_type: "double".to_string(),
            },
        ];
        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();
        assert_eq!(cfg.dim_schemas[0].dim_type, DimensionType::String);
        assert_eq!(cfg.dim_schemas[1].dim_type, DimensionType::Long);
        assert_eq!(cfg.dim_schemas[2].dim_type, DimensionType::Double);
    }

    #[test]
    fn security_properties_are_forwarded_managed_keys_dropped() {
        let mut spec = sample_spec();
        let props = &mut spec.io_config.consumer_properties;
        props.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        // A caller attempt to force auto-commit ON must be dropped.
        props.insert("enable.auto.commit".to_string(), "true".to_string());
        props.insert("group.id".to_string(), "attacker-group".to_string());
        // The `bootstrap.servers` ALIAS `metadata.broker.list` must be dropped
        // too (Codex R37 round-5) — otherwise it could win non-deterministic
        // config application and repoint the consumer at another cluster.
        props.insert(
            "metadata.broker.list".to_string(),
            "attacker-cluster:9092".to_string(),
        );
        // Native-code-loading keys must be dropped, not forwarded.
        props.insert(
            "plugin.library.paths".to_string(),
            "/tmp/evil.so".to_string(),
        );
        props.insert(
            "ssl.engine.location".to_string(),
            "/tmp/evil-engine.so".to_string(),
        );

        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();

        assert_eq!(
            cfg.additional_properties
                .get("security.protocol")
                .map(String::as_str),
            Some("SASL_SSL")
        );
        assert_eq!(
            cfg.additional_properties
                .get("sasl.mechanism")
                .map(String::as_str),
            Some("PLAIN")
        );
        // Managed keys must NOT leak through to additional_properties.
        assert!(!cfg.additional_properties.contains_key("enable.auto.commit"));
        assert!(!cfg.additional_properties.contains_key("group.id"));
        assert!(!cfg.additional_properties.contains_key("bootstrap.servers"));
        assert!(
            !cfg.additional_properties
                .contains_key("metadata.broker.list"),
            "the bootstrap.servers alias metadata.broker.list must be dropped (R37 round-5)"
        );
        // Forbidden native-code-loading keys must NOT leak through.
        assert!(
            !cfg.additional_properties
                .contains_key("plugin.library.paths")
        );
        assert!(
            !cfg.additional_properties
                .contains_key("ssl.engine.location")
        );
        // The runtime's own (stable) group id wins.
        assert_eq!(cfg.group_id, "ferrodruid-s", "group_id = {}", cfg.group_id);
    }

    /// Codex R29 F2: the rebalance-protocol keys are runtime-MANAGED. The
    /// consumer's duplication guarantee (no in-process re-delivery on a
    /// partition-count increase) rests on the `cooperative-sticky` pin in
    /// `create_stream_consumer`; a spec-supplied eager
    /// `partition.assignment.strategy` — or a `group.protocol=consumer`
    /// (KIP-848), under which librdkafka REJECTS the pinned strategy at
    /// client creation — must therefore be dropped, not forwarded.
    #[test]
    fn rebalance_protocol_keys_are_managed_not_forwarded() {
        let mut spec = sample_spec();
        let props = &mut spec.io_config.consumer_properties;
        props.insert(
            "partition.assignment.strategy".to_string(),
            "range".to_string(),
        );
        props.insert("group.protocol".to_string(), "consumer".to_string());

        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();

        assert!(
            !cfg.additional_properties
                .contains_key("partition.assignment.strategy"),
            "spec-supplied assignment strategy must not reach librdkafka"
        );
        assert!(
            !cfg.additional_properties.contains_key("group.protocol"),
            "spec-supplied group.protocol must not reach librdkafka"
        );
    }

    /// Codex R36: `max.poll.interval.ms` is FORWARDED (not stripped) so the
    /// client builder can FLOOR it to `max(caller, 300000)`. The runtime keeps
    /// the caller's raw value in `additional_properties`; `build_client_config`
    /// (kafka-io) then computes the effective floored value. Forwarding is what
    /// lets a legitimately-large value survive while the tiny value is raised.
    #[test]
    fn max_poll_interval_is_forwarded_for_flooring() {
        let mut spec = sample_spec();
        spec.io_config
            .consumer_properties
            .insert("max.poll.interval.ms".to_string(), "900000".to_string());

        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();

        assert_eq!(
            cfg.additional_properties
                .get("max.poll.interval.ms")
                .map(String::as_str),
            Some("900000"),
            "max.poll.interval.ms must be forwarded so build_client_config can floor it (R36)"
        );
    }

    /// Codex R37: the EXACT-pinned data-affecting properties (`check.crcs` —
    /// integrity; `allow.auto.create.topics` — topic completeness;
    /// `group.instance.id` — static-membership identity) are runtime-MANAGED:
    /// a spec-supplied value must be DROPPED from `additional_properties` so
    /// the pin (applied last in `build_client_config`, or the correct default
    /// absence) always wins. `topic.metadata.refresh.interval.ms` is instead
    /// FORWARDED (to be clamped, like `max.poll.interval.ms`), asserted here so
    /// a legitimately-faster caller value is not stripped. Pairs with the
    /// kafka-io `build_client_config` tests that assert the effective values.
    #[test]
    fn r37_data_affecting_properties_are_managed_not_forwarded() {
        let mut spec = sample_spec();
        let props = &mut spec.io_config.consumer_properties;
        // The exact pathological values from the R37 findings.
        props.insert("check.crcs".to_string(), "false".to_string());
        props.insert("allow.auto.create.topics".to_string(), "true".to_string());
        props.insert(
            "group.instance.id".to_string(),
            "static-member-1".to_string(),
        );
        // A legitimately FASTER refresh (short-retention topic) must survive to
        // build_client_config for clamping, NOT be stripped (R37 F2 / round-3).
        props.insert(
            "topic.metadata.refresh.interval.ms".to_string(),
            "30000".to_string(),
        );

        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();

        for key in [
            "check.crcs",
            "allow.auto.create.topics",
            "group.instance.id",
        ] {
            assert!(
                !cfg.additional_properties.contains_key(key),
                "spec-supplied {key} must be dropped (exact-pinned, runtime-managed, R37)"
            );
        }
        assert_eq!(
            cfg.additional_properties
                .get("topic.metadata.refresh.interval.ms")
                .map(String::as_str),
            Some("30000"),
            "a legitimately-faster metadata refresh must be forwarded for clamping, not \
             stripped (R37 F2)"
        );
    }

    /// Codex R38: the protocol-version negotiation knobs are runtime-MANAGED
    /// so a spec cannot DOWNGRADE the Fetch protocol below the version at which
    /// `isolation.level=read_committed` is honoured. `api.version.request=false`
    /// (+ a pre-0.11 `broker.version.fallback`, or a forced negotiation timeout)
    /// makes librdkafka speak a pre-v4 Fetch, which a down-conversion-enabled
    /// Kafka 2.x/3.x broker serves as READ_UNCOMMITTED — silently publishing
    /// aborted transactional records the schemaFp was stamped `read_committed`
    /// for (KIP-98). All three deprecated pre-0.10-broker knobs must therefore
    /// be DROPPED from `additional_properties` so the pins (applied last in
    /// `build_client_config`, or the correct default absence) always win. Pairs
    /// with the kafka-io `client_config_pins_r38_protocol_negotiation` test that
    /// asserts the effective pinned values.
    #[test]
    fn r38_protocol_downgrade_properties_are_managed_not_forwarded() {
        let mut spec = sample_spec();
        let props = &mut spec.io_config.consumer_properties;
        // The exact protocol-downgrade attack from the R38 finding: disable
        // negotiation and claim a pre-transactional broker so read_committed
        // silently degrades to read_uncommitted on a down-converting broker.
        props.insert("api.version.request".to_string(), "false".to_string());
        props.insert("broker.version.fallback".to_string(), "0.9.0".to_string());
        props.insert(
            "api.version.fallback.ms".to_string(),
            "604800000".to_string(),
        );

        let runtime = KafkaSupervisorRuntime::new("s".to_string(), spec);
        let cfg = runtime.build_consumer_config();

        for key in [
            "api.version.request",
            "broker.version.fallback",
            "api.version.fallback.ms",
        ] {
            assert!(
                !cfg.additional_properties.contains_key(key),
                "spec-supplied {key} must be dropped (protocol-downgrade guard, runtime-managed, R38)"
            );
        }
    }

    #[test]
    fn runtime_start_stop() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut runtime = KafkaSupervisorRuntime::new("test-sup".to_string(), sample_spec());

            assert!(!runtime.is_running());
            assert_eq!(runtime.running_task_count(), 0);

            runtime.start(3);
            assert!(runtime.is_running());
            assert_eq!(runtime.running_task_count(), 3);

            runtime.stop().await;
            assert!(!runtime.is_running());
            assert_eq!(runtime.running_task_count(), 0);
        });
    }

    #[test]
    fn runtime_default_max_rows() {
        let mut spec = sample_spec();
        spec.tuning_config = None;
        let runtime = KafkaSupervisorRuntime::new("test".to_string(), spec);
        let config = runtime.build_consumer_config();
        assert_eq!(config.max_rows_per_segment, 5_000_000);
    }
}
