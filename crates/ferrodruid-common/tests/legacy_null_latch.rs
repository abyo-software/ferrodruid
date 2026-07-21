// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — process-global latch semantics.
//!
//! Lives in its own integration binary (own process) because the latch is
//! set-once per process: these are the ONLY tests allowed to touch it in
//! this binary, and they must all agree on the observed value.
//!
//! Contract under test (H3): the flag is **immutable from FIRST
//! observation** — a plain read latches the ANSI default, so a later
//! `init(true)` can never flip what an earlier reader already answered
//! with (per-query consistency).  The init-first path ("first init
//! wins") is covered by the module doctest and by the legacy-latched
//! test binaries (`legacy_null_empty_aggs`, `legacy_null_sql_e2e`,
//! `legacy_null_ingest_coercion`), each of which inits `true` before any
//! read in its own process.

use ferrodruid_common::null_mode::{
    init_legacy_null_mode, init_legacy_null_mode_serve, legacy_null_mode,
};

/// One test drives the whole latch lifecycle — split tests would race on
/// the process-global (test threads share it).
#[test]
fn first_observation_freezes_the_latch() {
    // An un-initialised READ observes ANSI...
    assert!(!legacy_null_mode(), "un-initialised latch reads ANSI");

    // ...and FREEZES it: the flag is immutable from first observation,
    // so a later init(true) must be a no-op that REPORTS the frozen
    // `false` (no reader may ever observe a flip mid-process).
    assert!(
        !init_legacy_null_mode(true),
        "init(true) after a first read must NOT flip the frozen latch"
    );
    assert!(!legacy_null_mode(), "the latch never flips mid-process");

    // The serve-init helper goes LOUD on the conflict so a serving
    // binary refuses to start half-ANSI/half-legacy...
    let err = init_legacy_null_mode_serve(true);
    assert!(
        err.is_err(),
        "serve-init with a conflicting value must fail loudly, got {err:?}"
    );

    // ...and accepts a matching (idempotent) re-init.
    assert_eq!(
        init_legacy_null_mode_serve(false),
        Ok(false),
        "serve-init with the already-latched value is fine"
    );
}
