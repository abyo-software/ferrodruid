<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroDruid WASM Plugin Examples

Three reference plugins that exercise every capability surface
provided by `ferrodruid-plugin-rt`:

| Plugin            | What it does                                           | Capabilities required |
|-------------------|--------------------------------------------------------|------------------------|
| `running_stddev`  | Welford online standard deviation aggregator           | (none)                |
| `http_jsonl`      | HTTP-JSONL input source — counts JSONL rows from a URL | `net`                 |
| `hmac_bearer`     | HMAC-SHA-256 bearer token authenticator                | (none)                |

## Building

```bash
rustup target add wasm32-unknown-unknown    # one-time per toolchain
./build_plugins.sh                          # builds + copies to dist/
```

Each plugin is a standalone Cargo package (`Cargo.toml` declares
`[workspace]` to detach from the parent FerroDruid workspace).
Build artifacts land under `<plugin>/target/wasm32-unknown-unknown/release/`;
`build_plugins.sh` copies the release `.wasm` into `dist/` and
prints the SHA-256 manifest each host should pin.

## Loading from the host

```rust
use ferrodruid_plugin_rt::{
    ModuleManifest, PluginCapabilities, PluginRuntime, RuntimeConfig,
    expected_sha256_hex,
};

let bytes = std::fs::read("crates/ferrodruid-plugin-rt/plugins/dist/running_stddev.wasm")?;
let manifest = ModuleManifest {
    name: "running-stddev".into(),
    version: "1.0.0".into(),
    sha256_hex: expected_sha256_hex(&bytes),
};
let mut rt = PluginRuntime::new()?;
rt.load(manifest, &bytes)?;

let mut inst = rt.instantiate(
    "running-stddev",
    PluginCapabilities::deny_all(),       // no capabilities for stddev
    RuntimeConfig::default(),             // 50M fuel + 16 MiB memory cap
)?;

let handle: i32 = inst.call("agg_new", ())?;
for v in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
    inst.call::<(i32, f64), ()>("agg_aggregate", (handle, v))?;
}
let stddev: f64 = inst.call("agg_finalize", handle)?;   // == 2.0
inst.call::<i32, ()>("agg_drop", handle)?;
```

## Plugin ABI contract

Every plugin MUST export:

| Symbol                       | Type                                  | Purpose                          |
|------------------------------|---------------------------------------|----------------------------------|
| `memory`                     | `Memory`                              | The plugin's linear memory       |
| `plugin_abi_version()`       | `func () -> i32`                      | Returns `1` (host-required ABI)  |
| `alloc(size)`                | `func i32 -> i32`                     | Byte-buffer allocator            |
| `dealloc(ptr, size)`         | `func (i32, i32) -> ()`               | Byte-buffer free                 |

Plus the plugin-type-specific exports defined in each plugin's
source comments.

### Why `plugin_abi_version` is a function

Rust→wasm32 lowers `pub static FOO: i32 = …` to a wasm `(global)`
whose **value is the linear-memory address** of the static, not the
integer payload.  Reading that global back from the host would yield
the meaningless pointer `1048608` instead of the `1` we want.  A
function returning the constant sidesteps the footgun.

## Host capability surface

| Host import                                      | Capability flag | Behaviour                                       |
|--------------------------------------------------|------------------|--------------------------------------------------|
| `ferro::ferro_log(level, ptr, len)`              | `log`            | Forward bytes to the host's tracing subscriber  |
| `ferro::ferro_clock_now_ms() -> i64`             | `clock`          | Wall-clock millis since UNIX epoch               |
| `ferro::ferro_random_u64() -> i64`               | `random`         | 64 bits of OS-provided randomness                |
| `ferro::ferro_http_get(url_ptr, url_len, out_ptr, out_cap) -> i32` | `net`            | Blocking HTTP GET (requires `net-capability` crate feature) |

Capabilities are **deny-by-default**; granting one is a per-instance
choice (`PluginCapabilities::deny_all().with_clock()`).  A plugin
that imports a host function whose capability has not been granted
fails at instantiate time with the typed
`PluginError::CapabilityDenied { name, capability }` error.

## Signature verification

`ModuleManifest::sha256_hex` is checked against the loaded bytes
**before** the module is compiled, so a tampered .wasm cannot be
silently substituted at runtime.  Production deployments should
also verify the manifest itself via cosign / Sigstore — that work is
tracked under `docs/known-limitations.md` EF-2.

## Rebuilding after a source change

The committed `dist/*.wasm` bytes MUST stay in lockstep with the
source under `<plugin>/src/`.  After changing any plugin source:

```bash
./build_plugins.sh
git add dist/ <plugin>/src/
git commit -m "feat(plugin-rt): bump <plugin> version + regenerate dist"
```

The end-to-end integration test
(`crates/ferrodruid-plugin-rt/tests/end_to_end.rs`) computes the
expected hash from the live `dist/*.wasm` bytes at test time, so
the test cannot drift away from the committed binaries.
