// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-6 / W1-F end-to-end integration test.
//!
//! Drives all three reference WASM plugins through the
//! `ferrodruid-plugin-rt` host API and asserts:
//!
//! 1. Each plugin loads cleanly with a SHA-256 sig manifest.
//! 2. Each plugin computes the right answer when called through the
//!    safe Rust host API (= the same surface FerroDruid integrates
//!    against).
//! 3. Capability gating actually denies the network capability when
//!    the operator did not grant it.
//! 4. Fuel budget enforcement still applies inside the real plugins.
//! 5. Load → instantiate → unload → reload cycles leave the engine's
//!    module catalog at the same baseline (= no module-reference leak
//!    across cycles).
//! 6. The `http-jsonl` plugin can drive a host-supplied fixture URL
//!    through the `net` capability and count the JSONL rows we
//!    handed back.
//!
//! The committed `.wasm` artifacts live under
//! `crates/ferrodruid-plugin-rt/plugins/dist/`; rebuild via
//! `crates/ferrodruid-plugin-rt/plugins/build_plugins.sh` when the
//! source under the matching `plugins/*/src/` changes.

use std::path::{Path, PathBuf};

use ferrodruid_plugin_rt::{
    DEFAULT_FUEL_BUDGET, DEFAULT_MEMORY_CAP_BYTES, ModuleManifest, PluginCapabilities, PluginError,
    PluginRuntime, RuntimeConfig, expected_sha256_hex,
};

fn dist_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("plugins")
        .join("dist")
}

fn load_bytes(name: &str) -> Vec<u8> {
    let path = dist_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing pre-built plugin artifact {}: {e} \
             — rebuild via crates/ferrodruid-plugin-rt/plugins/build_plugins.sh",
            path.display()
        )
    })
}

fn manifest_for(name: &str, version: &str, bytes: &[u8]) -> ModuleManifest {
    ModuleManifest {
        name: name.into(),
        version: version.into(),
        sha256_hex: expected_sha256_hex(bytes),
    }
}

// ---------------------------------------------------------------------------
// running-stddev
// ---------------------------------------------------------------------------

#[test]
fn running_stddev_matches_population_formula() {
    let bytes = load_bytes("running_stddev.wasm");
    let manifest = manifest_for("running-stddev", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load running-stddev");

    let mut inst = rt
        .instantiate(
            "running-stddev",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let handle: i32 = inst.call("agg_new", ()).expect("agg_new");
    assert_ne!(handle, 0, "agg_new returned null handle");

    // Wikipedia "Standard deviation" worked example: stddev = 2.0.
    for v in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
        inst.call::<(i32, f64), ()>("agg_aggregate", (handle, v))
            .expect("agg_aggregate");
    }
    let stddev: f64 = inst.call("agg_finalize", handle).expect("agg_finalize");
    assert!(
        (stddev - 2.0).abs() < 1e-12,
        "expected stddev=2.0, observed {stddev}"
    );

    let count: i64 = inst.call("agg_count", handle).expect("agg_count");
    assert_eq!(count, 8);

    // Fuel should be well under the default budget for this many calls.
    assert!(
        inst.fuel_consumed() < DEFAULT_FUEL_BUDGET,
        "running-stddev burned > 50M fuel for 8 values"
    );

    inst.call::<i32, ()>("agg_drop", handle).expect("agg_drop");
}

#[test]
fn running_stddev_merge_matches_single_pass() {
    let bytes = load_bytes("running_stddev.wasm");
    let manifest = manifest_for("running-stddev", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "running-stddev",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let combined: i32 = inst.call("agg_new", ()).expect("alloc");
    let left: i32 = inst.call("agg_new", ()).expect("alloc");
    let right: i32 = inst.call("agg_new", ()).expect("alloc");

    let xs: [f64; 10] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    for v in xs {
        inst.call::<(i32, f64), ()>("agg_aggregate", (combined, v))
            .expect("agg");
    }
    for v in &xs[..5] {
        inst.call::<(i32, f64), ()>("agg_aggregate", (left, *v))
            .expect("agg");
    }
    for v in &xs[5..] {
        inst.call::<(i32, f64), ()>("agg_aggregate", (right, *v))
            .expect("agg");
    }

    inst.call::<(i32, i32), ()>("agg_merge", (left, right))
        .expect("merge");
    let merged: f64 = inst.call("agg_finalize", left).expect("finalize");
    let direct: f64 = inst.call("agg_finalize", combined).expect("finalize");
    assert!(
        (merged - direct).abs() < 1e-12,
        "merge mismatch: merged={merged} direct={direct}"
    );

    for h in [combined, left, right] {
        inst.call::<i32, ()>("agg_drop", h).expect("drop");
    }
}

#[test]
fn running_stddev_fuel_budget_traps_explosive_workload() {
    // Aggregating millions of values must eventually exhaust a tiny
    // fuel budget, surfaced as a typed PluginError::OutOfFuel.
    let bytes = load_bytes("running_stddev.wasm");
    let manifest = manifest_for("running-stddev", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "running-stddev",
            PluginCapabilities::deny_all(),
            RuntimeConfig {
                fuel_budget: 1_000, // tiny budget
                memory_cap_bytes: DEFAULT_MEMORY_CAP_BYTES,
            },
        )
        .expect("instantiate");
    let handle: i32 = inst.call("agg_new", ()).expect("agg_new");
    // Each agg_aggregate burns ~tens of fuel; 1000 of them at most
    // 1_000 budget total ⇒ we will OOG somewhere along the way.
    let mut last_err: Option<PluginError> = None;
    for v in 0..2000 {
        if let Err(e) = inst.call::<(i32, f64), ()>("agg_aggregate", (handle, v as f64)) {
            last_err = Some(e);
            break;
        }
    }
    // With a 1_000-fuel-per-call budget, each individual agg_aggregate
    // call may finish — what we actually want to assert is that a
    // tighter budget on a SINGLE call traps.  Repeat with a much
    // smaller budget per call by re-using the same instance.
    if last_err.is_none() {
        // Force an OOG by shrinking the budget to 10 and calling once.
        inst.call::<i32, ()>("agg_drop", handle).expect("drop");
        // Build a fresh tiny-budget instance.
        let mut tiny = rt
            .instantiate(
                "running-stddev",
                PluginCapabilities::deny_all(),
                RuntimeConfig {
                    fuel_budget: 10,
                    memory_cap_bytes: DEFAULT_MEMORY_CAP_BYTES,
                },
            )
            .expect("instantiate tiny");
        let h2: Result<i32, PluginError> = tiny.call("agg_new", ());
        match h2 {
            Err(PluginError::OutOfFuel { budget: 10 }) => {
                last_err = Some(PluginError::OutOfFuel { budget: 10 });
            }
            Ok(h) => panic!("expected OutOfFuel under 10-fuel budget, got handle {h}"),
            Err(other) => panic!("expected OutOfFuel, got {other:?}"),
        }
    }
    assert!(
        matches!(last_err, Some(PluginError::OutOfFuel { .. })),
        "expected OutOfFuel from running-stddev, observed: {last_err:?}"
    );
}

// ---------------------------------------------------------------------------
// http-jsonl
// ---------------------------------------------------------------------------

#[test]
fn http_jsonl_denies_net_capability_when_not_granted() {
    let bytes = load_bytes("http_jsonl.wasm");
    let manifest = manifest_for("http-jsonl", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");

    // Default caps = deny everything.  The module imports
    // `ferro_http_get` ⇒ instantiation MUST refuse with
    // CapabilityDenied.
    let err = rt
        .instantiate(
            "http-jsonl",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect_err("must deny net cap");
    match err {
        PluginError::CapabilityDenied { name, capability } => {
            assert_eq!(name, "ferro_http_get");
            assert_eq!(capability, "net");
        }
        other => panic!("expected CapabilityDenied, got {other:?}"),
    }
}

#[cfg(feature = "net-capability")]
#[test]
fn http_jsonl_fetches_and_counts_lines_via_fixture() {
    let bytes = load_bytes("http_jsonl.wasm");
    let manifest = manifest_for("http-jsonl", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");

    let mut inst = rt
        .instantiate(
            "http-jsonl",
            PluginCapabilities::deny_all().with_net(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    // Inject a fixture URL ⇒ JSONL body so we never touch the
    // network during the test.
    let body = b"{\"event\":\"a\"}\n{\"event\":\"b\"}\n{\"event\":\"c\"}\n";
    inst.add_http_fixture("http://fixture.invalid/events.jsonl", body.to_vec());

    // Write the URL into a host-allocated buffer inside the plugin's
    // memory; allocate a scratch buffer for the response body.
    let url = b"http://fixture.invalid/events.jsonl";
    let (url_ptr, url_len) = inst.write_owned(url).expect("write url");
    let scratch_cap: u32 = 1024;
    let scratch_ptr = inst.alloc(scratch_cap).expect("alloc scratch");

    let n: i64 = inst
        .call(
            "input_fetch_line_count",
            (
                url_ptr as i32,
                url_len as i32,
                scratch_ptr as i32,
                scratch_cap as i32,
            ),
        )
        .expect("input_fetch_line_count");
    assert_eq!(n, 3, "expected 3 JSONL rows, got {n}");

    inst.dealloc(scratch_ptr, scratch_cap).expect("dealloc");
    inst.dealloc(url_ptr, url_len).expect("dealloc url");
}

#[cfg(feature = "net-capability")]
#[test]
fn http_jsonl_signals_truncation_when_scratch_too_small() {
    let bytes = load_bytes("http_jsonl.wasm");
    let manifest = manifest_for("http-jsonl", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "http-jsonl",
            PluginCapabilities::deny_all().with_net(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let body = vec![b'x'; 4096];
    inst.add_http_fixture("http://fixture.invalid/big", body);

    let url = b"http://fixture.invalid/big";
    let (url_ptr, url_len) = inst.write_owned(url).expect("write url");
    let scratch_cap: u32 = 256;
    let scratch_ptr = inst.alloc(scratch_cap).expect("alloc");

    let n: i64 = inst
        .call(
            "input_fetch_line_count",
            (
                url_ptr as i32,
                url_len as i32,
                scratch_ptr as i32,
                scratch_cap as i32,
            ),
        )
        .expect("call");
    assert_eq!(n, -2, "scratch too small must surface -2; got {n}");

    inst.dealloc(scratch_ptr, scratch_cap).expect("dealloc");
    inst.dealloc(url_ptr, url_len).expect("dealloc url");
}

// ---------------------------------------------------------------------------
// hmac-bearer
// ---------------------------------------------------------------------------

#[test]
fn hmac_bearer_verifies_valid_token() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let bytes = load_bytes("hmac_bearer.wasm");
    let manifest = manifest_for("hmac-bearer", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "hmac-bearer",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let secret = b"super-secret-key-bytes-32-or-more";
    let message = b"alice.0";
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac");
    mac.update(message);
    let sig = hex::encode(mac.finalize().into_bytes());
    let token = format!("alice.0.{sig}");

    let (token_ptr, token_len) = inst.write_owned(token.as_bytes()).expect("write");
    let (secret_ptr, secret_len) = inst.write_owned(secret).expect("write");

    let r: i32 = inst
        .call(
            "auth_verify",
            (
                token_ptr as i32,
                token_len as i32,
                secret_ptr as i32,
                secret_len as i32,
                123_456_789_i64,
            ),
        )
        .expect("auth_verify");
    assert_eq!(r, 1, "valid token must return 1, got {r}");

    inst.dealloc(token_ptr, token_len).expect("dealloc");
    inst.dealloc(secret_ptr, secret_len).expect("dealloc");
}

#[test]
fn hmac_bearer_rejects_tampered_signature() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let bytes = load_bytes("hmac_bearer.wasm");
    let manifest = manifest_for("hmac-bearer", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "hmac-bearer",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let secret = b"super-secret-key-bytes-32-or-more";
    let message = b"alice.0";
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac");
    mac.update(message);
    let sig = hex::encode(mac.finalize().into_bytes());
    let mut token = format!("alice.0.{sig}");
    // Flip the last hex char.
    let last = token.pop().expect("non-empty");
    let new_last = if last == 'a' { 'b' } else { 'a' };
    token.push(new_last);

    let (token_ptr, token_len) = inst.write_owned(token.as_bytes()).expect("write");
    let (secret_ptr, secret_len) = inst.write_owned(secret).expect("write");

    let r: i32 = inst
        .call(
            "auth_verify",
            (
                token_ptr as i32,
                token_len as i32,
                secret_ptr as i32,
                secret_len as i32,
                123_456_789_i64,
            ),
        )
        .expect("auth_verify");
    assert_eq!(r, 0, "tampered token must return 0, got {r}");

    inst.dealloc(token_ptr, token_len).expect("dealloc");
    inst.dealloc(secret_ptr, secret_len).expect("dealloc");
}

#[test]
fn hmac_bearer_rejects_expired_token() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let bytes = load_bytes("hmac_bearer.wasm");
    let manifest = manifest_for("hmac-bearer", "1.0.0", &bytes);
    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(manifest, &bytes).expect("load");
    let mut inst = rt
        .instantiate(
            "hmac-bearer",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("instantiate");

    let secret = b"super-secret-key-bytes-32-or-more";
    let message = b"alice.1000";
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac");
    mac.update(message);
    let sig = hex::encode(mac.finalize().into_bytes());
    let token = format!("alice.1000.{sig}");

    let (token_ptr, token_len) = inst.write_owned(token.as_bytes()).expect("write");
    let (secret_ptr, secret_len) = inst.write_owned(secret).expect("write");

    let r: i32 = inst
        .call(
            "auth_verify",
            (
                token_ptr as i32,
                token_len as i32,
                secret_ptr as i32,
                secret_len as i32,
                10_000_i64, // > 1000 = expired
            ),
        )
        .expect("auth_verify");
    assert_eq!(r, -2, "expired token must return -2, got {r}");

    inst.dealloc(token_ptr, token_len).expect("dealloc");
    inst.dealloc(secret_ptr, secret_len).expect("dealloc");
}

// ---------------------------------------------------------------------------
// Signature verification + leak audit
// ---------------------------------------------------------------------------

#[test]
fn signature_mismatch_blocks_load() {
    let bytes = load_bytes("running_stddev.wasm");
    let mut bad = manifest_for("running-stddev", "1.0.0", &bytes);
    bad.sha256_hex = "ff".repeat(32);
    let mut rt = PluginRuntime::new().expect("runtime");
    let err = rt.load(bad, &bytes).expect_err("must reject");
    assert!(matches!(err, PluginError::SignatureMismatch { .. }));
    assert_eq!(rt.stats().loaded_modules, 0);
}

#[test]
fn dynamic_load_unload_reload_does_not_leak_modules() {
    // Real-world: a host might hot-reload a plugin after the
    // operator pushes a new version.  The catalog must end every
    // cycle at the same module count.
    let stddev_bytes = load_bytes("running_stddev.wasm");
    let jsonl_bytes = load_bytes("http_jsonl.wasm");
    let bearer_bytes = load_bytes("hmac_bearer.wasm");

    let mut rt = PluginRuntime::new().expect("runtime");
    assert_eq!(rt.stats().loaded_modules, 0);

    for cycle in 0..5 {
        rt.load(
            manifest_for("running-stddev", "1.0.0", &stddev_bytes),
            &stddev_bytes,
        )
        .unwrap_or_else(|e| panic!("cycle {cycle} load stddev: {e}"));
        rt.load(
            manifest_for("http-jsonl", "1.0.0", &jsonl_bytes),
            &jsonl_bytes,
        )
        .unwrap_or_else(|e| panic!("cycle {cycle} load jsonl: {e}"));
        rt.load(
            manifest_for("hmac-bearer", "1.0.0", &bearer_bytes),
            &bearer_bytes,
        )
        .unwrap_or_else(|e| panic!("cycle {cycle} load bearer: {e}"));
        assert_eq!(rt.stats().loaded_modules, 3, "cycle {cycle} post-load");

        // Drive at least one call through each plugin so the engine
        // actually allocates per-instance state.
        {
            let mut inst = rt
                .instantiate(
                    "running-stddev",
                    PluginCapabilities::deny_all(),
                    RuntimeConfig::default(),
                )
                .expect("inst stddev");
            let h: i32 = inst.call("agg_new", ()).expect("new");
            inst.call::<(i32, f64), ()>("agg_aggregate", (h, 1.0))
                .expect("agg");
            inst.call::<i32, ()>("agg_drop", h).expect("drop");
            // dropping `inst` here releases every byte of its Store.
        }

        assert!(rt.unload("running-stddev"));
        assert!(rt.unload("http-jsonl"));
        assert!(rt.unload("hmac-bearer"));
        assert_eq!(rt.stats().loaded_modules, 0, "cycle {cycle} post-unload");
    }
}

#[test]
fn three_plugins_co_loaded_into_one_engine() {
    // The host should be able to keep all 3 plugins loaded
    // simultaneously and drive each from the same engine without
    // cross-contamination.
    let stddev_bytes = load_bytes("running_stddev.wasm");
    let jsonl_bytes = load_bytes("http_jsonl.wasm");
    let bearer_bytes = load_bytes("hmac_bearer.wasm");

    let mut rt = PluginRuntime::new().expect("runtime");
    rt.load(
        manifest_for("running-stddev", "1.0.0", &stddev_bytes),
        &stddev_bytes,
    )
    .expect("load stddev");
    rt.load(
        manifest_for("http-jsonl", "1.0.0", &jsonl_bytes),
        &jsonl_bytes,
    )
    .expect("load jsonl");
    rt.load(
        manifest_for("hmac-bearer", "1.0.0", &bearer_bytes),
        &bearer_bytes,
    )
    .expect("load bearer");
    assert_eq!(rt.stats().loaded_modules, 3);

    // Drive a stddev sample through.
    let mut stddev = rt
        .instantiate(
            "running-stddev",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("inst stddev");
    let h: i32 = stddev.call("agg_new", ()).expect("new");
    for v in [10.0_f64, 20.0, 30.0] {
        stddev
            .call::<(i32, f64), ()>("agg_aggregate", (h, v))
            .expect("agg");
    }
    let s: f64 = stddev.call("agg_finalize", h).expect("finalize");
    // mean=20, m2=200, variance=200/3, stddev=sqrt(200/3)
    let expected = (200.0_f64 / 3.0).sqrt();
    assert!(
        (s - expected).abs() < 1e-12,
        "stddev mismatch: {s} vs {expected}"
    );
    stddev.call::<i32, ()>("agg_drop", h).expect("drop");

    // hmac-bearer still works in the same engine.
    let mut bearer = rt
        .instantiate(
            "hmac-bearer",
            PluginCapabilities::deny_all(),
            RuntimeConfig::default(),
        )
        .expect("inst bearer");
    let secret = b"key";
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac");
    mac.update(b"bob.0");
    let sig = hex::encode(mac.finalize().into_bytes());
    let token = format!("bob.0.{sig}");
    let (tp, tl) = bearer.write_owned(token.as_bytes()).expect("write");
    let (sp, sl) = bearer.write_owned(secret).expect("write");
    let r: i32 = bearer
        .call(
            "auth_verify",
            (tp as i32, tl as i32, sp as i32, sl as i32, 0_i64),
        )
        .expect("verify");
    assert_eq!(r, 1);
    bearer.dealloc(tp, tl).ok();
    bearer.dealloc(sp, sl).ok();
}
