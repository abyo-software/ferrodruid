// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Smoke test for the `parse_druid_sql` density guard against every saved
//! fuzz-farm artifact.
//!
//! Each input must (a) not panic / stack-overflow / OOM the parser, and
//! (b) complete within a small wall-clock budget. The verdict (Ok vs Err)
//! is deliberately left open — most archived inputs are rejected by the
//! density caps, but a few are flagged "falsepos" (the 24 h fuzz runner
//! attributed cumulative RSS or wall-clock to a benign input) and parse
//! successfully. Both outcomes are acceptable as long as the parser does
//! not regress into the original DoS path.
//!
//! The test is the local regression net against future sqlparser bumps or
//! guard edits: a regression that drops an artifact back into sqlparser
//! proper trips the per-input budget instead of waiting for evo-x2's
//! 24 h fuzz cycle to re-flag it.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Per-input wall-clock budget. The density guard is a single byte-scan
/// that returns in microseconds; the budget is two orders of magnitude
/// past that. The original DoS paths burned 100 ms – 30 s of CPU before
/// being killed by libFuzzer, so any regression past the guard easily
/// blows this cap.
///
/// Debug builds get a larger budget because rustc compiles the recursive-
/// descent expression parser without inlining: a 200-byte input that
/// returns in ~10 ms under `--release` takes ~400 ms under `cargo test`.
/// The budget is still well under the multi-second wall-clock seen on the
/// pre-fix DoS paths.
const PER_INPUT_BUDGET: Duration = if cfg!(debug_assertions) {
    Duration::from_millis(1500)
} else {
    Duration::from_millis(200)
};

/// Stack size for each per-input worker thread. The `MAX_SQL_PAREN_DEPTH`
/// guard in `parser.rs` is tuned against the 8 MiB main-thread stack that
/// Linux gives a libFuzzer binary; the Rust test harness defaults to a
/// much smaller per-test stack (typically 2 MiB) which would let a
/// previously-fixed pathological input overflow inside `cargo test`
/// without actually indicating a regression. We size each worker to the
/// production budget so the test reflects what the fuzz runner sees.
const PARSE_STACK: usize = 8 * 1024 * 1024;

fn check_dir(dir: &Path) -> usize {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    let mut checked = 0usize;
    for entry in entries {
        let path = entry.expect("readdir entry").path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<unnamed>")
            .to_string();
        let data = std::fs::read(&path).expect("read artifact");
        let Ok(s) = std::str::from_utf8(&data) else {
            // Non-UTF8 inputs are dropped by the fuzz harness before
            // reaching the parser, so they cannot exercise it.
            continue;
        };

        let owned = s.to_string();
        let handle = std::thread::Builder::new()
            .name(format!("parse-{name}"))
            .stack_size(PARSE_STACK)
            .spawn(move || {
                let start = Instant::now();
                let _ = ferrodruid_sql::parser::parse_druid_sql(&owned);
                start.elapsed()
            })
            .expect("spawn worker");
        let elapsed = handle
            .join()
            .unwrap_or_else(|_| panic!("artifact {name} panicked inside parse_druid_sql"));

        assert!(
            elapsed <= PER_INPUT_BUDGET,
            "artifact {}/{} ({} B) took {elapsed:?} to parse (budget {PER_INPUT_BUDGET:?}); \
             a density-guard regression has dropped the input back into sqlparser proper",
            dir.display(),
            name,
            data.len()
        );
        checked += 1;
    }
    checked
}

#[test]
fn all_saved_fuzz_artifacts_are_handled_fast() {
    let manifest: PathBuf = env!("CARGO_MANIFEST_DIR").into();
    // `known-crash/` is the curated archive of every historically caught
    // input; `artifacts/` is libFuzzer's live drop zone for any input
    // not yet triaged. Both must clear the budget.
    let archived = manifest.join("../../fuzz/known-crash/fuzz_sql_parse");
    let live = manifest.join("../../fuzz/artifacts/fuzz_sql_parse");
    let n_archived = check_dir(&archived);
    let n_live = check_dir(&live);
    assert!(
        n_archived + n_live > 0,
        "no fuzz artifacts examined — directory layout changed? \
         archived={} live={}",
        archived.display(),
        live.display()
    );
}
