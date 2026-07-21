// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//
// CL-A1 progress: thin `cargo test` wrapper around the
// `tests/jepsen-rs/` scaffold. Marked `#[ignore]` because it requires
// docker + the docker compose plugin and a release `ferrodruid`
// binary; invoke via:
//
// ```sh
// cargo test -p ferrodruid-cluster --test jepsen_scaffold \
//     -- --ignored --nocapture
// ```
//
// This is a discovery hook only — the real harness is the standalone
// `tests/jepsen-rs/` Cargo package driven by `tests/jepsen-rs/run.sh`.
// We delegate to that script rather than duplicating the docker
// orchestration here.

use std::path::PathBuf;
use std::process::Command;

fn jepsen_dir() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    let mut p = PathBuf::from(manifest);
    p.pop(); // crates/ferrodruid-cluster -> crates
    p.pop(); // crates -> repo
    p.push("tests");
    p.push("jepsen-rs");
    p
}

#[test]
#[ignore]
fn jepsen_scaffold_partition_lin_kv_register() {
    // Require docker; if it's missing, fail loudly with a clear
    // skip message so CI environments without docker know to skip
    // this test rather than misreading a generic IO error.
    let docker_present = Command::new("docker")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(
        docker_present,
        "jepsen scaffold requires docker on PATH; skipping is not \
         automatic — use `cargo test -- --skip jepsen` to exclude."
    );

    let dir = jepsen_dir();
    let script = dir.join("run.sh");
    assert!(script.exists(), "missing tests/jepsen-rs/run.sh");

    let status = Command::new("bash")
        .arg(&script)
        .current_dir(&dir)
        .status()
        .expect("spawn run.sh");
    assert!(
        status.success(),
        "jepsen-rs scaffold reported a checker violation; see \
         tests/jepsen-rs/out/checker.json"
    );
}
