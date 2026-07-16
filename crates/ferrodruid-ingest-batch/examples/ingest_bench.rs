// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//! Ingest-throughput micro-bench: 1M JSON rows through BatchIngester::ingest.
use ferrodruid_ingest_batch::BatchIngester;
fn main() {
    const N: usize = 1_000_000;
    let rows: Vec<serde_json::Value> = (0..N)
        .map(|i| {
            serde_json::json!({
                "__time": 1_700_000_000_000_i64 + i as i64,
                "site": format!("site_{}", i % 50),
                "device": format!("dev_{}", i % 500),
                "value": (i % 1000) as f64 * 0.5,
            })
        })
        .collect();
    let ingester = BatchIngester::new(
        "bench".into(),
        "__time".into(),
        vec!["site".into(), "device".into()],
        vec![serde_json::json!({"type":"doubleSum","name":"value","fieldName":"value"})],
    );
    let t = std::time::Instant::now();
    let seg = ingester.ingest(rows).expect("ingest");
    let el = t.elapsed();
    println!(
        "ingest 1M rows: {:.2}s ({:.1} K rows/s), rows={}",
        el.as_secs_f64(),
        N as f64 / el.as_secs_f64() / 1000.0,
        seg.num_rows
    );
}
