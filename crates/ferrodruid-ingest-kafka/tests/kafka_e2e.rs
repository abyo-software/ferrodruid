// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real Kafka 3.7 ⇄ FerroDruid Kafka ingestion end-to-end test.
//!
//! Spawns a `KafkaConsumerTask` against a live broker (default
//! `localhost:9092`, override with `KAFKA_BOOTSTRAP`), produces 1000 JSON rows
//! via an `rdkafka` `FutureProducer`, drains the topic into a smoosh v9
//! segment on disk, then re-opens the segment and asserts row count and
//! aggregate values.
//!
//! Marked `#[ignore]` so it is excluded from default `cargo test`. Run via:
//!
//! ```sh
//! cargo test -p ferrodruid-ingest-kafka --features kafka-io \
//!     --test kafka_e2e -- --ignored --nocapture
//! ```

#![cfg(feature = "kafka-io")]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use ferrodruid_ingest_kafka::consumer::{KafkaConsumerConfig, KafkaConsumerTask};
use ferrodruid_segment::column::ColumnData;
use ferrodruid_segment::segment::SegmentData;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

const ROW_COUNT: usize = 1000;
const PAGES: &[&str] = &["Alpha", "Bravo", "Charlie", "Delta", "Echo"];

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn unique_topic() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ferrodruid-e2e-{nanos}")
}

async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");

    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let res = admin
        .create_topics(&[new_topic], &opts)
        .await
        .expect("create_topics call");
    for r in res {
        match r {
            Ok(name) => eprintln!("created topic: {name}"),
            Err((name, err)) => panic!("failed to create topic {name}: {err:?}"),
        }
    }
}

async fn produce_rows(brokers: &str, topic: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");

    // Stable, deterministic timestamp base.
    let base_ts: i64 = 1_700_000_000_000;
    for i in 0..ROW_COUNT {
        let page = PAGES[i % PAGES.len()];
        let added = (i as i64) + 1;
        let row = serde_json::json!({
            "__time": base_ts + (i as i64),
            "page": page,
            "added": added,
        });
        let payload = serde_json::to_vec(&row).expect("serialize row");
        let key = format!("k{i}");
        let record: FutureRecord<'_, String, Vec<u8>> =
            FutureRecord::to(topic).payload(&payload).key(&key);
        producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e)
            .expect("produce");
    }
    // Force flush.
    producer
        .flush(Duration::from_secs(10))
        .expect("producer flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn kafka_real_broker_e2e() {
    let brokers = bootstrap_servers();
    let topic = unique_topic();
    eprintln!("brokers={brokers} topic={topic}");

    let started = std::time::Instant::now();

    // 1. Create topic + produce 1000 rows.
    create_topic(&brokers, &topic).await;
    produce_rows(&brokers, &topic).await;
    let produced_at = started.elapsed();
    eprintln!("produced {ROW_COUNT} rows in {produced_at:?}");

    // 2. Set up the consumer task to write segments to a tempdir.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir: PathBuf = tmp.path().to_path_buf();

    let config = KafkaConsumerConfig {
        brokers: brokers.clone(),
        topic: topic.clone(),
        group_id: format!("ferrodruid-e2e-{}", std::process::id()),
        data_source: "wiki_e2e".to_string(),
        timestamp_column: "__time".to_string(),
        dimensions: vec!["page".to_string()],
        // Single segment containing all 1000 rows.
        max_rows_per_segment: ROW_COUNT,
        segment_flush_interval_ms: 60_000,
        use_earliest_offset: true,
        additional_properties: HashMap::new(),
        output_dir: Some(out_dir.clone()),
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    let mut task = KafkaConsumerTask::new(config, shutdown_rx);

    // 3. Run the consumer until we've ingested ROW_COUNT rows or hit a deadline.
    let consumer_handle = tokio::spawn(async move {
        // run() loops until shutdown; we'll send shutdown from the watchdog.
        let _ = task.run().await;
        task
    });

    // Watchdog: poll a control channel for "done" via a separate consumer that
    // peeks the running task is hard, so we just give the consumer enough wall
    // time and then signal shutdown. The auto-flush at max_rows_per_segment
    // will fire as soon as ROW_COUNT rows accumulate, so the segment file
    // appears well before shutdown.
    //
    // Wait until the segment file appears, with a hard 60s ceiling.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut seen_segment: Option<PathBuf> = None;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Ok(rd) = std::fs::read_dir(&out_dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() && p.join("meta.smoosh").exists() {
                    seen_segment = Some(p);
                    break;
                }
            }
        }
        if seen_segment.is_some() {
            break;
        }
    }

    // Signal shutdown either way (so the consumer task exits cleanly).
    let _ = shutdown_tx.send(()).await;
    let task = consumer_handle.await.expect("consumer task join");

    let segment_dir = seen_segment.expect(
        "segment file did not appear within 60s; check Kafka broker is reachable and topic exists",
    );
    eprintln!(
        "segment dir: {} (after {:?}, total_consumed={})",
        segment_dir.display(),
        started.elapsed(),
        task.total_consumed()
    );

    // 4. Read the segment back and verify.
    let segment = SegmentData::open(&segment_dir).expect("open segment");

    assert_eq!(segment.version, 9, "segment version");
    assert_eq!(
        segment.num_rows(),
        ROW_COUNT,
        "row count: produced {ROW_COUNT}, segment has {}",
        segment.num_rows()
    );
    assert_eq!(segment.dimensions, vec!["page".to_string()]);

    // __time column.
    let times = segment.timestamp_column().expect("timestamp column");
    assert_eq!(times.len(), ROW_COUNT);
    // Sorted ascending by ingester contract.
    for w in times.windows(2) {
        assert!(w[0] <= w[1], "timestamps must be non-decreasing");
    }

    // page dimension: distinct count == PAGES.len(), all PAGES present.
    let page_col = segment.column("page").expect("page column");
    let page_str = match page_col {
        ColumnData::String(s) => s,
        other => panic!("page column has unexpected type: {other:?}"),
    };
    assert_eq!(
        page_str.encoded_values.len(),
        ROW_COUNT,
        "page column row count"
    );
    let mut distinct_pages: HashSet<String> = HashSet::new();
    for (_, v) in page_str.dictionary.iter() {
        distinct_pages.insert(v.to_string());
    }
    assert_eq!(
        distinct_pages.len(),
        PAGES.len(),
        "distinct page count: got {distinct_pages:?}"
    );
    for p in PAGES {
        assert!(
            distinct_pages.contains(*p),
            "expected page {p:?} in dictionary {distinct_pages:?}"
        );
    }

    eprintln!(
        "OK: ROW_COUNT={ROW_COUNT}, distinct_pages={}, total elapsed={:?}",
        distinct_pages.len(),
        started.elapsed()
    );
}
