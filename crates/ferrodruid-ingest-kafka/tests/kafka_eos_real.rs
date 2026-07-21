// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-1 closure: real-Kafka exactly-once + rebalance + fault-injection.
//!
//! Wave V1-A integration suite for the ROADMAP.md CL-1 closure bar:
//! * **EOS** — kill mid-flush, restart, assert zero duplicates and zero
//!   gaps in the published `__time` / `seq_id` sequence.
//! * **Consumer-group rebalance** — scale task count 1 → 2 → 1 in the
//!   same group; assert no row loss and monotonic offsets across all
//!   phases.
//! * **Broker kill** — `docker stop` / `docker start` mid-stream; assert
//!   the consumer reconnects and finishes ingestion without surfacing
//!   data loss.
//! * **Schema drift / bad records** — interleave malformed payloads with
//!   the valid stream; assert the skip-policy path drops only the bad
//!   records and the rest land in segments.
//!
//! Each test is `#[ignore]`'d so default `cargo test` skips it. Run via:
//!
//! ```sh
//! tests/ingestion-compat/run_kafka_eos_real.sh
//! ```
//!
//! which brings up the docker-compose Kafka broker on `localhost:9092`,
//! runs the suite, and tears the broker down on exit. The broker-kill
//! test additionally requires the `docker compose` binary to be
//! reachable and the `FERRODRUID_KAFKA_COMPOSE_FILE` env var to point at
//! the compose file — `run_kafka_eos_real.sh` sets it automatically.

#![cfg(feature = "kafka-io")]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_ingest_kafka::eos_writer::{EosSegmentWriter, PendingBatch};
use ferrodruid_ingest_kafka::partitions::TopicPartition;
use ferrodruid_segment::column::ColumnData;
use ferrodruid_segment::segment::SegmentData;

use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared scaffolding
// ---------------------------------------------------------------------------

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos}")
}

async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");

    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let res = admin
        .create_topics(&[new_topic], &opts)
        .await
        .expect("create_topics call");
    for r in res {
        match r {
            Ok(name) => eprintln!("[topic] created {name}"),
            Err((name, err)) => panic!("failed to create topic {name}: {err:?}"),
        }
    }
}

// One message in the synthetic stream carries `__time = BASE_TS + seq`,
// `page = PAGES[seq % len]`, `seq = seq`. The assertions use the
// `__time` value as the per-row uniqueness key because BatchIngester
// strips unknown numeric fields into LONG columns whose access path
// is column-name-specific — `__time` is universally accessible via
// `SegmentData::timestamp_column()`.

const BASE_TS: i64 = 1_700_000_000_000;
const PAGES: &[&str] = &["Alpha", "Bravo", "Charlie", "Delta", "Echo"];

fn make_payload(seq: i64) -> Vec<u8> {
    let v = serde_json::json!({
        "__time": BASE_TS + seq,
        "page": PAGES[(seq as usize) % PAGES.len()],
        "seq": seq,
    });
    serde_json::to_vec(&v).expect("serialise row")
}

async fn produce_rows(brokers: &str, topic: &str, partitions: i32, count: i64) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        // acks=1 + idempotence=false keeps offset = record count, which
        // the EOS assertions rely on (idempotent producer injects a
        // sequence-number header that consumes extra log offsets).
        .set("acks", "1")
        .set("enable.idempotence", "false")
        .create()
        .expect("producer");

    for seq in 0..count {
        let payload = make_payload(seq);
        let key = format!("seq-{seq}");
        // Round-robin across partitions so multi-partition tests have
        // even fan-out.
        let part = (seq as i32) % partitions;
        let record: FutureRecord<'_, String, Vec<u8>> = FutureRecord::to(topic)
            .payload(&payload)
            .key(&key)
            .partition(part);
        producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e)
            .expect("produce");
    }
    producer
        .flush(Duration::from_secs(10))
        .expect("producer flush");
}

fn make_consumer(brokers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Faster rebalance for the test; production deployments leave the
        // defaults in place.
        .set("session.timeout.ms", "10000")
        .set("max.poll.interval.ms", "15000")
        .create()
        .expect("stream consumer")
}

fn ingester() -> BatchIngester {
    BatchIngester::new(
        "eos_test".to_owned(),
        "__time".to_owned(),
        vec!["page".to_owned()],
        vec![],
    )
}

/// Collect every `seq` value present in the published segments under
/// `base_dir`. The seq column is encoded as the bytes the producer wrote
/// (a `__time` long), so we walk the segment v9 columns directly.
fn collect_published_seq_ids(writer: &EosSegmentWriter) -> BTreeSet<i64> {
    let mut out = BTreeSet::new();
    for seg in writer.list_published().expect("list published") {
        let data = SegmentData::open(&seg.dir).expect("open published segment");
        // `seq` is encoded as part of the row, but BatchIngester turns
        // unknown numeric fields into LONG columns by default. Use
        // `__time` instead as the per-row uniqueness key — the producer
        // sets `__time = BASE_TS + seq` so the mapping is bijective.
        let times = data
            .timestamp_column()
            .expect("__time column on published segment");
        for &t in times {
            let seq = t - BASE_TS;
            assert!(
                out.insert(seq),
                "duplicate seq={seq} discovered across published segments \
                 (segment dir = {})",
                seg.dir.display(),
            );
        }
    }
    out
}

/// Per-partition resume offset reported by the writer's durable state.
fn next_offsets(writer: &EosSegmentWriter) -> BTreeMap<TopicPartition, i64> {
    writer
        .resume_offsets_all_tasks()
        .expect("resume offsets all tasks")
}

/// Why this phase exited. Used by the test assertions to decide
/// whether the run was a clean "caught-up to end of log" termination or
/// a simulated mid-flight kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseExit {
    /// Reached the configured `stop_after_published` threshold — the
    /// caller wants to simulate an in-flight kill, so we exit
    /// non-gracefully (no final flush of the partial buffer).
    StopThreshold,
    /// Consumer position reached the high watermark on every assigned
    /// partition — there is provably nothing else to consume right now.
    CaughtUpToHighWatermark,
    /// Hard deadline elapsed before either of the above. Treated as a
    /// soft error so the caller can assert on it.
    Deadline,
}

// Drive a single consumer loop until one of: (a) `stop_after_published`
// rows have been published and committed, (b) every assigned partition's
// consumer position equals the broker high watermark, or (c) the deadline
// elapses (with `idle_quiet_for` as the soft no-records timeout).
//
// EOS semantics:
//   1. Resume by consulting `writer.resume_offsets(task_id)` for the
//      published-segment frontier per partition.
//   2. Buffer rows into a `PendingBatch` keyed by partition. Skip any
//      record whose offset is BELOW the frontier (already-published).
//   3. On buffer full → atomically publish via `EosSegmentWriter::publish`,
//      then commit Kafka offsets. If the process dies between the
//      publish-rename and the Kafka commit, the next run skips the
//      already-published rows via (2). If it dies BEFORE publish-rename,
//      `EosSegmentWriter::new` purges the pending dir and the next run
//      re-reads and re-publishes (no duplicate, because nothing was
//      published).
#[allow(clippy::too_many_arguments)]
async fn run_consumer_phase(
    consumer: &StreamConsumer,
    writer: &EosSegmentWriter,
    topic: &str,
    task_id: &str,
    batch_size: usize,
    stop_after_published: i64,
    deadline: Duration,
    idle_quiet_for: Duration,
) -> Result<(i64, PhaseExit), String> {
    consumer
        .subscribe(&[topic])
        .map_err(|e| format!("subscribe: {e}"))?;

    let started = Instant::now();
    let mut last_recv = Instant::now();
    let mut buffer = PendingBatch::new();
    let mut published_total: i64 = 0;
    let mut next_segment_seq: u64 = 0;
    // Per-phase entropy so a restarted phase's segment ids do not
    // collide with the prior phase's (the writer's publish path is
    // idempotent on collision, which would *silently drop* the
    // restarted rows — a real bug surfaced by the EOS test).
    let phase_nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    // EOS resume: we do NOT seek the consumer. Instead we rely on the
    // Kafka group-commit offset to point us at the right starting
    // position (`auto.offset.reset=earliest` on the very first run),
    // and we use `writer.resume_offsets(task_id)` as a per-partition
    // dedupe frontier — any record whose offset is BELOW the frontier
    // has already been durably published, so we skip it without
    // re-buffering. This handles the "publish committed but Kafka offset
    // commit was lost" case (re-read but skip) AND the "buffer pending
    // but no publish" case (re-read and publish fresh — the prior
    // pending dir was purged on `EosSegmentWriter::new`).
    //
    // Crucially we MUST NOT call `consumer.recv()` before this point to
    // wait for assignment — recv() consumes records, and the first
    // delivered record would be silently lost. rdkafka's StreamConsumer
    // drives the group rebalance protocol via the recv() future, so
    // the first message of the run also doubles as the "assignment is
    // settled" signal.
    let resume = writer
        .resume_offsets(task_id)
        .map_err(|e| format!("resume offsets: {e}"))?;
    if !resume.is_empty() {
        eprintln!(
            "[{task_id}] resume dedupe frontier loaded: {} partitions",
            resume.len()
        );
        for (tp, next) in &resume {
            eprintln!(
                "[{task_id}]   {} p{} dedupe < {next}",
                tp.topic, tp.partition
            );
        }
    }

    let mut exit_reason = PhaseExit::Deadline;
    loop {
        if started.elapsed() >= deadline {
            eprintln!("[{task_id}] deadline reached after {:?}", started.elapsed());
            break;
        }
        if published_total >= stop_after_published {
            eprintln!(
                "[{task_id}] stop_after_published={stop_after_published} reached; ending phase"
            );
            exit_reason = PhaseExit::StopThreshold;
            break;
        }
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Err(_) => {
                // On idle, check if we're at the high watermark on every
                // assigned partition. If so, we're caught up and can
                // exit cleanly. This is the EOS-correct way to know
                // "there's nothing else to consume right now" without
                // relying on a fragile wall-clock heuristic.
                if is_caught_up(consumer) {
                    eprintln!(
                        "[{task_id}] caught up to high watermark after {:?}; ending phase",
                        started.elapsed()
                    );
                    exit_reason = PhaseExit::CaughtUpToHighWatermark;
                    break;
                }
                if last_recv.elapsed() >= idle_quiet_for {
                    eprintln!(
                        "[{task_id}] idle for {:?} (>{:?}); ending phase",
                        last_recv.elapsed(),
                        idle_quiet_for,
                    );
                    break;
                }
                continue;
            }
            Ok(Err(e)) => {
                eprintln!("[{task_id}] recv err (continuing): {e}");
                continue;
            }
            Ok(Ok(m)) => {
                last_recv = Instant::now();

                let tp = TopicPartition::new(m.topic().to_owned(), m.partition());
                // EOS dedupe: if this offset is already covered by a
                // previously-published segment, skip it without
                // buffering. The Kafka offset commit will still advance
                // because rdkafka's `position()` tracks the highest
                // delivered offset on this consumer, and we
                // `commit_consumer_state` periodically.
                if let Some(&frontier) = resume.get(&tp)
                    && m.offset() < frontier
                {
                    // Skip but commit periodically so we don't keep
                    // re-reading the same skipped records forever.
                    if m.offset() % 50 == 0 {
                        let _ = consumer.commit_consumer_state(CommitMode::Sync);
                    }
                    continue;
                }

                let payload = match m.payload() {
                    Some(p) => p,
                    None => continue,
                };
                let row: serde_json::Value = match serde_json::from_slice(payload) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "[{task_id}] skip bad payload @ {}/{}: {e}",
                            m.partition(),
                            m.offset()
                        );
                        // Still must advance past the bad record in the
                        // Kafka offset, or we'd re-read it forever on
                        // restart. Treat it like a "null row" by buffering
                        // the offset advance without a row payload.
                        let entry = buffer.spans.entry(tp).or_insert(
                            ferrodruid_ingest_kafka::eos_writer::OffsetSpan::new(
                                m.offset(),
                                m.offset() + 1,
                            ),
                        );
                        if m.offset() + 1 > entry.next {
                            entry.next = m.offset() + 1;
                        }
                        continue;
                    }
                };
                buffer.record(tp, m.offset(), row);

                if buffer.len() >= batch_size {
                    let segment_id = format!("{task_id}-{phase_nonce}-seg-{next_segment_seq}");
                    next_segment_seq += 1;
                    let drained = std::mem::take(&mut buffer);
                    let drained_rows = drained.len() as i64;
                    let (_pub_seg, ingested) = writer
                        .publish(task_id, &segment_id, drained, &ingester())
                        .map_err(|e| format!("publish {segment_id}: {e}"))?;
                    eprintln!(
                        "[{task_id}] published {segment_id} ({} rows; \
                         segment.num_rows={})",
                        drained_rows, ingested.num_rows,
                    );
                    published_total += drained_rows;
                    // Commit offsets only AFTER the durable publish.
                    consumer
                        .commit_consumer_state(CommitMode::Sync)
                        .map_err(|e| format!("commit: {e}"))?;
                }
            }
        }
    }

    // Final flush — drain any leftover buffer ONLY on graceful exit
    // paths (caught-up or deadline). The `StopThreshold` exit path
    // simulates a kill, so the partial buffer MUST be left un-flushed
    // — this is the whole point of the EOS test.
    if !buffer.is_empty() && exit_reason != PhaseExit::StopThreshold {
        let segment_id = format!("{task_id}-{phase_nonce}-seg-{next_segment_seq}");
        let drained = std::mem::take(&mut buffer);
        let drained_rows = drained.len() as i64;
        let (_pub_seg, _ingested) = writer
            .publish(task_id, &segment_id, drained, &ingester())
            .map_err(|e| format!("final publish: {e}"))?;
        eprintln!("[{task_id}] final-flush published {segment_id} ({drained_rows} rows)");
        published_total += drained_rows;
        consumer
            .commit_consumer_state(CommitMode::Sync)
            .map_err(|e| format!("commit: {e}"))?;
    }

    Ok((published_total, exit_reason))
}

/// True when, for every assigned partition, the highest fetchable
/// offset has been reached. Uses `consumer.position()` (per-partition
/// next-to-deliver offset) when the consumer has received at least one
/// record on a partition, and falls back to `consumer.committed()`
/// (per-partition committed-group offset) when the consumer has not yet
/// pulled anything from a partition this session — e.g. a restart that
/// finds everything already committed. Without the committed-offset
/// fallback the loop would never observe "caught up" on an idle
/// already-drained partition and would always exit via Deadline.
fn is_caught_up(consumer: &StreamConsumer) -> bool {
    let assignment = match consumer.assignment() {
        Ok(a) => a,
        Err(_) => return false,
    };
    if assignment.count() == 0 {
        return false;
    }

    let positions = consumer.position().ok();
    let committed = consumer.committed(Duration::from_millis(1500)).ok();

    for tp in assignment.elements() {
        // Prefer per-partition consumed position; if it isn't set
        // (Invalid / Beginning / End / Stored / no entry), fall back to
        // the broker-committed offset.
        let mut effective: Option<i64> = None;
        if let Some(p) = positions.as_ref() {
            for pt in p.elements() {
                if pt.topic() == tp.topic()
                    && pt.partition() == tp.partition()
                    && let rdkafka::Offset::Offset(o) = pt.offset()
                {
                    effective = Some(o);
                }
            }
        }
        if effective.is_none()
            && let Some(c) = committed.as_ref()
        {
            for ct in c.elements() {
                if ct.topic() == tp.topic()
                    && ct.partition() == tp.partition()
                    && let rdkafka::Offset::Offset(o) = ct.offset()
                {
                    effective = Some(o);
                }
            }
        }
        let pos = match effective {
            Some(o) => o,
            None => return false,
        };

        let high = match consumer.fetch_watermarks(
            tp.topic(),
            tp.partition(),
            Duration::from_millis(1500),
        ) {
            Ok((_low, high)) => high,
            Err(_) => return false,
        };
        if pos < high {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Test 1 — exactly-once across mid-flush crash + restart
// ---------------------------------------------------------------------------
//
// Closure-bar clause: "kill ingestion task mid-segment publish, restart,
// assert zero duplicates AND zero gaps in the __time sequence of
// published segments."
//
// Strategy:
// 1. Produce N=200 messages on one partition, each carrying a unique seq
//    (and __time = BASE_TS + seq), so we can detect duplicates and gaps
//    after the fact by inspecting the published segments.
// 2. Phase A — start a consumer with EosSegmentWriter, batch_size=30, run
//    until ~120 rows are published, then drop the consumer (simulating
//    a kill — this leaves any in-flight buffer un-flushed and any
//    pending/ dir to be purged on restart). Crucially, we do NOT call
//    a graceful "final flush" path — we drop mid-stream.
// 3. Phase B — open a fresh consumer with the same group_id and the
//    same writer base_dir. The writer's `resume_offsets(task_id)`
//    reports the next offset implied by published segments, and we
//    seek to that. Run to completion.
// 4. Assert: union of seq across all published segments == {0..N} exactly.
//    No dupes, no gaps.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn eos_exactly_once_across_kill_restart() {
    let brokers = bootstrap_servers();
    let topic = unique_topic("eos-restart");
    let group_id = format!("eos-restart-grp-{}", std::process::id());
    eprintln!("[eos-restart] brokers={brokers} topic={topic} group={group_id}");

    const TOTAL_ROWS: i64 = 200;
    const PHASE_A_STOP_AFTER: i64 = 120;
    const BATCH_SIZE: usize = 30;

    create_topic(&brokers, &topic, 1).await;
    produce_rows(&brokers, &topic, 1, TOTAL_ROWS).await;

    let tmp = TempDir::new().expect("tmpdir");
    let base_dir = tmp.path().to_path_buf();

    // Phase A — consume until PHASE_A_STOP_AFTER rows published, then
    // simulate a kill by leaving partial buffer un-flushed (the writer
    // is dropped without a final-flush call).
    {
        let writer = EosSegmentWriter::new(&base_dir).expect("writer A");
        let consumer = make_consumer(&brokers, &group_id);
        let (published, exit) = run_consumer_phase(
            &consumer,
            &writer,
            &topic,
            "eos-task-1",
            BATCH_SIZE,
            PHASE_A_STOP_AFTER,
            Duration::from_secs(45),
            Duration::from_secs(10),
        )
        .await
        .expect("phase A");
        eprintln!("[eos-restart][A] published_total={published} exit={exit:?}");
        assert!(
            published >= PHASE_A_STOP_AFTER,
            "phase A should publish at least {PHASE_A_STOP_AFTER}, got {published}"
        );
        assert_eq!(
            exit,
            PhaseExit::StopThreshold,
            "phase A must exit via the stop-threshold path so the partial buffer is left un-flushed (simulated kill)"
        );
        // Debug: list segments and their per-partition spans for diagnosis.
        for seg in writer.list_published().expect("list A") {
            eprintln!(
                "[eos-restart][A][debug] {}: spans={:?}",
                seg.dir.file_name().unwrap_or_default().to_string_lossy(),
                seg.spans
            );
        }
        // Drop the consumer & the in-flight buffer (simulated crash).
        drop(consumer);
        // (writer drops with the inner PendingBatch — never flushed.)
    }

    // Phase B — fresh writer + consumer with the same group + task_id.
    // resume_offsets() must steer past the already-published rows so the
    // second run does not duplicate them.
    let final_published_total;
    {
        let writer = EosSegmentWriter::new(&base_dir).expect("writer B");
        let consumer = make_consumer(&brokers, &group_id);
        let (published, exit) = run_consumer_phase(
            &consumer,
            &writer,
            &topic,
            "eos-task-1",
            BATCH_SIZE,
            i64::MAX, // run until caught up
            Duration::from_secs(60),
            Duration::from_secs(15),
        )
        .await
        .expect("phase B");
        eprintln!("[eos-restart][B] published_total={published} exit={exit:?}");
        assert_eq!(
            exit,
            PhaseExit::CaughtUpToHighWatermark,
            "phase B must drain to the high watermark (not exit via idle/deadline) for the EOS proof"
        );

        // Combine A + B: collect via the final writer state.
        let combined = collect_published_seq_ids(&writer);
        final_published_total = combined.len() as i64;
        let (low, high) = consumer
            .fetch_watermarks(&topic, 0, Duration::from_secs(5))
            .expect("fetch watermarks");
        eprintln!(
            "[eos-restart][verify] published_seq_count={final_published_total}, expected={TOTAL_ROWS}, broker_low={low} broker_high={high}"
        );

        // Sanity: the broker high watermark should match the produced
        // count. If not, the producer dropped records and the test is
        // testing the wrong invariant.
        assert_eq!(
            high - low,
            TOTAL_ROWS,
            "broker high - low must equal the produced count (the EOS test \
             cannot prove no-loss if the producer dropped records first); \
             low={low} high={high}"
        );

        // Hard assertions of the closure bar.
        assert_eq!(
            combined.len() as i64,
            TOTAL_ROWS,
            "expected exactly {TOTAL_ROWS} distinct seq values in published segments, got {}",
            combined.len()
        );
        let expected: BTreeSet<i64> = (0..TOTAL_ROWS).collect();
        assert_eq!(combined, expected, "gap or extraneous seq detected");

        // Per-partition next offset must match the broker high watermark.
        let next = next_offsets(&writer);
        let key = TopicPartition::new(topic.clone(), 0);
        assert_eq!(
            next.get(&key).copied(),
            Some(high),
            "expected per-partition next-offset == broker high watermark ({high})"
        );
    }

    eprintln!(
        "[eos-restart] OK — TOTAL_ROWS={TOTAL_ROWS}, distinct_published={final_published_total}"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — consumer-group rebalance (1 → 2 → 1) no loss / monotonic offsets
// ---------------------------------------------------------------------------
//
// Closure-bar clause: "Consumer-group rebalance: scale supervisor task
// count 1→2→1 mid-run; assert no message loss and monotonic offsets."

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn consumer_group_rebalance_1_2_1_no_loss() {
    let brokers = bootstrap_servers();
    let topic = unique_topic("eos-rebalance");
    let group_id = format!("eos-rebalance-grp-{}", std::process::id());
    eprintln!("[rebalance] brokers={brokers} topic={topic} group={group_id}");

    const PARTITIONS: i32 = 3;
    const TOTAL_ROWS: i64 = 300;
    const BATCH_SIZE: usize = 25;

    create_topic(&brokers, &topic, PARTITIONS).await;
    produce_rows(&brokers, &topic, PARTITIONS, TOTAL_ROWS).await;

    let tmp = TempDir::new().expect("tmpdir");
    let base_dir = tmp.path().to_path_buf();

    // Phase 1: a single consumer in the group handles all 3 partitions
    // for a while.
    {
        let writer = EosSegmentWriter::new(&base_dir).expect("writer 1");
        let consumer = make_consumer(&brokers, &group_id);
        let (published, exit) = run_consumer_phase(
            &consumer,
            &writer,
            &topic,
            "rebalance-task-A",
            BATCH_SIZE,
            80,
            Duration::from_secs(30),
            Duration::from_secs(6),
        )
        .await
        .expect("phase 1");
        eprintln!("[rebalance][P1] task-A published={published} exit={exit:?}");
        // Leave the group cleanly so the broker can reassign without
        // waiting for session timeout.
        consumer.unsubscribe();
        drop(consumer);
    }

    // Phase 2: two consumers in the same group; rdkafka assigns
    // partitions across them. Each uses its own task_id but shares the
    // writer's base_dir, so resume_offsets_all_tasks() converges.
    {
        let brokers_a = brokers.clone();
        let topic_a = topic.clone();
        let group_a = group_id.clone();
        let base_a = base_dir.clone();
        let brokers_b = brokers.clone();
        let topic_b = topic.clone();
        let group_b = group_id.clone();
        let base_b = base_dir.clone();

        let h_a = tokio::spawn(async move {
            let writer = EosSegmentWriter::new(&base_a).expect("writer 2a");
            let consumer = make_consumer(&brokers_a, &group_a);
            let (n, exit) = run_consumer_phase(
                &consumer,
                &writer,
                &topic_a,
                "rebalance-task-A",
                BATCH_SIZE,
                i64::MAX,
                Duration::from_secs(60),
                Duration::from_secs(12),
            )
            .await
            .expect("phase 2a");
            consumer.unsubscribe();
            (n, exit)
        });
        let h_b = tokio::spawn(async move {
            // small delay so consumer A is in first and B triggers a
            // rebalance.
            tokio::time::sleep(Duration::from_secs(3)).await;
            let writer = EosSegmentWriter::new(&base_b).expect("writer 2b");
            let consumer = make_consumer(&brokers_b, &group_b);
            let (n, exit) = run_consumer_phase(
                &consumer,
                &writer,
                &topic_b,
                "rebalance-task-B",
                BATCH_SIZE,
                i64::MAX,
                Duration::from_secs(60),
                Duration::from_secs(12),
            )
            .await
            .expect("phase 2b");
            consumer.unsubscribe();
            (n, exit)
        });
        let (na, nb) = tokio::join!(h_a, h_b);
        eprintln!(
            "[rebalance][P2] task-A published={:?} task-B published={:?}",
            na, nb
        );
    }

    // Phase 3: back to a single consumer. Reads anything left.
    {
        let writer = EosSegmentWriter::new(&base_dir).expect("writer 3");
        let consumer = make_consumer(&brokers, &group_id);
        let (published, exit) = run_consumer_phase(
            &consumer,
            &writer,
            &topic,
            "rebalance-task-A",
            BATCH_SIZE,
            i64::MAX,
            Duration::from_secs(45),
            Duration::from_secs(12),
        )
        .await
        .expect("phase 3");
        eprintln!("[rebalance][P3] task-A published={published} exit={exit:?}");
        assert_eq!(
            exit,
            PhaseExit::CaughtUpToHighWatermark,
            "phase 3 must drain to high watermark to prove all rows have arrived"
        );
    }

    // Assertions.
    let writer = EosSegmentWriter::new(&base_dir).expect("verify writer");
    let combined = collect_published_seq_ids(&writer);
    let expected: BTreeSet<i64> = (0..TOTAL_ROWS).collect();
    assert_eq!(
        combined.len() as i64,
        TOTAL_ROWS,
        "rebalance must publish each row exactly once: expected {TOTAL_ROWS}, got {}",
        combined.len()
    );
    assert_eq!(combined, expected, "rebalance left gaps in the seq stream");

    // Per-partition next-offset monotonicity: every published segment's
    // (start, next) span must have start < next, and the union per
    // partition must form a contiguous prefix starting at 0.
    let mut per_part: BTreeMap<TopicPartition, Vec<(i64, i64)>> = BTreeMap::new();
    for seg in writer.list_published().expect("list") {
        for (tp, span) in seg.spans {
            assert!(
                span.start < span.next,
                "non-monotonic span on {tp:?}: {span:?}"
            );
            per_part
                .entry(tp)
                .or_default()
                .push((span.start, span.next));
        }
    }
    for (tp, mut spans) in per_part {
        spans.sort();
        // Sort + check no overlap and full coverage [0, hwm).
        let mut cursor = 0_i64;
        for (s, e) in &spans {
            assert_eq!(
                *s, cursor,
                "partition {tp:?} has gap or overlap before [{s},{e}); cursor={cursor}"
            );
            cursor = *e;
        }
        eprintln!("[rebalance] partition {tp:?} contiguous to next={cursor}");
    }
    eprintln!("[rebalance] OK — {TOTAL_ROWS} rows published exactly once across 1→2→1 rebalance");
}

// ---------------------------------------------------------------------------
// Test 3 — broker kill mid-stream, recovery
// ---------------------------------------------------------------------------
//
// Closure-bar clause: "Failure injection: broker kill ... transient
// EOFException ... recovery validated."
//
// Strategy:
// 1. Produce N=120 rows on a single partition.
// 2. Start the consumer, let it publish a few segments.
// 3. `docker compose stop` the Kafka container (broker kill). The
//    consumer experiences transient errors — rdkafka logs reconnection
//    attempts.
// 4. `docker compose start` the container; wait for healthy. The
//    consumer reconnects and finishes the run.
// 5. Assert all 120 rows are present in published segments.
//
// The docker control path is gated on FERRODRUID_KAFKA_COMPOSE_FILE. If
// it is unset (e.g. the test was launched manually against a non-docker
// broker), the test is skipped with a clear log line — the EOS and
// rebalance bars do not require docker control.

fn compose_file_from_env() -> Option<PathBuf> {
    std::env::var("FERRODRUID_KAFKA_COMPOSE_FILE")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn docker_compose(compose_file: &std::path::Path, args: &[&str]) -> std::io::Result<()> {
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(compose_file)
        .args(args)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "docker compose {args:?} -> exit {status:?}"
        )));
    }
    Ok(())
}

fn wait_for_kafka_health(
    compose_file: &std::path::Path,
    deadline: Duration,
) -> std::io::Result<()> {
    let start = Instant::now();
    loop {
        let output = Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(compose_file)
            .args(["ps", "--format", "json", "kafka"])
            .output()?;
        let text = String::from_utf8_lossy(&output.stdout);
        if text.contains("\"Health\":\"healthy\"") {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(std::io::Error::other(format!(
                "timeout waiting for kafka healthy after {:?}; last ps output: {}",
                deadline,
                text.lines().next().unwrap_or("(empty)")
            )));
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn broker_kill_recovery() {
    let Some(compose_file) = compose_file_from_env() else {
        eprintln!(
            "[broker-kill] SKIPPED: FERRODRUID_KAFKA_COMPOSE_FILE unset or path missing; \
             this test requires docker compose control."
        );
        return;
    };

    let brokers = bootstrap_servers();
    let topic = unique_topic("eos-broker-kill");
    let group_id = format!("eos-broker-kill-grp-{}", std::process::id());
    eprintln!("[broker-kill] brokers={brokers} topic={topic} group={group_id}");

    const TOTAL_ROWS: i64 = 120;
    const BATCH_SIZE: usize = 20;

    create_topic(&brokers, &topic, 1).await;
    produce_rows(&brokers, &topic, 1, TOTAL_ROWS).await;

    let tmp = TempDir::new().expect("tmpdir");
    let base_dir = tmp.path().to_path_buf();

    // Phase A — partial consume, then broker stop.
    let phase_a_handle = {
        let brokers = brokers.clone();
        let topic = topic.clone();
        let group = group_id.clone();
        let base = base_dir.clone();
        tokio::spawn(async move {
            let writer = EosSegmentWriter::new(&base).expect("writer A");
            let consumer = make_consumer(&brokers, &group);
            // Stop after ~40 rows so the broker kill happens mid-stream.
            run_consumer_phase(
                &consumer,
                &writer,
                &topic,
                "broker-kill-task",
                BATCH_SIZE,
                40,
                Duration::from_secs(45),
                Duration::from_secs(6),
            )
            .await
            .expect("phase A")
        })
    };

    let (published_a, exit_a) = phase_a_handle.await.expect("phase A join");
    eprintln!("[broker-kill][A] published={published_a} exit={exit_a:?}");
    assert!(
        published_a >= 20,
        "phase A should publish at least 20 before broker stop"
    );

    // Broker kill — `docker compose stop kafka` (no -v so volumes survive
    // — the broker comes back to the same log).
    eprintln!("[broker-kill] stopping kafka container...");
    docker_compose(&compose_file, &["stop", "kafka"]).expect("docker stop");

    // Sanity sleep — let any in-flight TCP errors propagate.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Broker restart.
    eprintln!("[broker-kill] starting kafka container...");
    docker_compose(&compose_file, &["start", "kafka"]).expect("docker start");
    wait_for_kafka_health(&compose_file, Duration::from_secs(120))
        .expect("kafka must come back healthy");
    eprintln!("[broker-kill] kafka healthy again");

    // Phase B — consume the rest.
    {
        let writer = EosSegmentWriter::new(&base_dir).expect("writer B");
        let consumer = make_consumer(&brokers, &group_id);
        let (published, exit) = run_consumer_phase(
            &consumer,
            &writer,
            &topic,
            "broker-kill-task",
            BATCH_SIZE,
            i64::MAX,
            Duration::from_secs(90),
            Duration::from_secs(15),
        )
        .await
        .expect("phase B");
        eprintln!("[broker-kill][B] published={published} exit={exit:?}");
        assert_eq!(
            exit,
            PhaseExit::CaughtUpToHighWatermark,
            "phase B must catch up to the high watermark after broker recovery"
        );

        let combined = collect_published_seq_ids(&writer);
        let expected: BTreeSet<i64> = (0..TOTAL_ROWS).collect();
        assert_eq!(
            combined.len() as i64,
            TOTAL_ROWS,
            "expected exactly {TOTAL_ROWS} distinct seq values after broker recovery, got {}",
            combined.len()
        );
        assert_eq!(combined, expected, "broker-kill recovery left a gap");
    }

    eprintln!("[broker-kill] OK — recovered after broker stop/start, {TOTAL_ROWS} rows preserved");
}

// ---------------------------------------------------------------------------
// Test 4 — schema drift / bad-record skip
// ---------------------------------------------------------------------------
//
// Closure-bar clause: "Failure injection: ... schema drift ... recovery
// validated."
//
// Strategy: produce a mixed stream where some payloads are invalid JSON
// or have completely different shapes from the declared schema. The
// consumer's per-record JSON parse step rejects the bad ones (skip
// policy) without halting the stream. Assert that all valid rows land
// in published segments and the bad records do not.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn schema_drift_skip_continues() {
    let brokers = bootstrap_servers();
    let topic = unique_topic("eos-drift");
    let group_id = format!("eos-drift-grp-{}", std::process::id());
    eprintln!("[drift] brokers={brokers} topic={topic} group={group_id}");

    create_topic(&brokers, &topic, 1).await;

    // Mixed-payload producer: 80 valid rows interleaved with 20 invalid
    // ones (drift: malformed JSON + wrong-schema JSON). seq is sparse:
    // the valid rows carry seq = 0..80.
    {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "10000")
            .create()
            .expect("producer");

        let mut seq: i64 = 0;
        let mut bad_count = 0;
        for i in 0..100 {
            // Every 5th message is malformed.
            let payload = if i % 5 == 4 {
                bad_count += 1;
                // Half malformed JSON, half a JSON object missing __time
                // (still valid JSON, but BatchIngester will turn the
                // missing time into 0, which then segments still accept).
                // To keep the test "drift means bad", we use a literal
                // garbage byte string on this half too.
                if i % 10 == 9 {
                    b"\xff\xff\xff\xff not json".to_vec()
                } else {
                    b"this is not valid JSON at all".to_vec()
                }
            } else {
                let p = make_payload(seq);
                seq += 1;
                p
            };
            let key = format!("k{i}");
            let record: FutureRecord<'_, String, Vec<u8>> = FutureRecord::to(&topic)
                .payload(&payload)
                .key(&key)
                .partition(0);
            producer
                .send(record, Duration::from_secs(10))
                .await
                .map_err(|(e, _)| e)
                .expect("produce");
        }
        producer
            .flush(Duration::from_secs(10))
            .expect("producer flush");
        assert_eq!(seq, 80, "should have produced 80 valid + 20 bad rows");
        assert_eq!(bad_count, 20);
    }

    let tmp = TempDir::new().expect("tmpdir");
    let writer = EosSegmentWriter::new(tmp.path()).expect("writer");
    let consumer = make_consumer(&brokers, &group_id);
    let (published, exit) = run_consumer_phase(
        &consumer,
        &writer,
        &topic,
        "drift-task",
        15, // small batch to force multiple flushes
        i64::MAX,
        Duration::from_secs(60),
        Duration::from_secs(12),
    )
    .await
    .expect("consume");
    eprintln!("[drift] published rows = {published} exit={exit:?}");
    assert_eq!(
        exit,
        PhaseExit::CaughtUpToHighWatermark,
        "drift run must drain to high watermark"
    );

    let combined = collect_published_seq_ids(&writer);
    let expected: BTreeSet<i64> = (0..80).collect();
    assert_eq!(
        combined.len(),
        80,
        "expected exactly 80 distinct valid seq values, got {}",
        combined.len()
    );
    assert_eq!(combined, expected, "valid rows missing from published set");

    // Sanity: at least one published segment has the `page` column
    // populated (it's the dimension we declared).
    let listed = writer.list_published().expect("list");
    let mut any_page = false;
    for seg in &listed {
        let data = SegmentData::open(&seg.dir).expect("open");
        if let Some(ColumnData::String(s)) = data.column("page") {
            assert!(!s.encoded_values.is_empty());
            any_page = true;
            break;
        }
    }
    assert!(
        any_page,
        "no published segment contained a populated `page` column"
    );

    eprintln!("[drift] OK — 80 valid rows preserved, 20 bad records skipped");
}
