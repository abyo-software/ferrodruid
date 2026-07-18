#!/usr/bin/env bash
# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Rebuild every reference plugin to wasm32-unknown-unknown release,
# copy the artifacts into dist/, and print the SHA-256 of each so the
# operator can pin the expected manifest hash in their host config.
#
# Prerequisites:
#   rustup target add wasm32-unknown-unknown   # one-time per toolchain
#
# Usage:
#   ./crates/ferrodruid-plugin-rt/plugins/build_plugins.sh
#
# The committed dist/*.wasm bytes MUST stay in lockstep with the
# committed source — re-run this script after every plugin source
# change and commit the refreshed binaries in the same patch.

set -euo pipefail

PLUGINS_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PLUGINS_DIR"

mkdir -p dist

for plugin in running_stddev http_jsonl hmac_bearer; do
    echo "==> building $plugin"
    (cd "$plugin" && cargo build --target wasm32-unknown-unknown --release)
    # The crate's package name is `ferrodruid-plugin-<plugin-with-dashes>`,
    # so the built artifact is named with underscores -> dashes mapping.
    name="ferrodruid_plugin_${plugin}"
    case "$plugin" in
        http_jsonl) name="ferrodruid_plugin_http_jsonl" ;;
        hmac_bearer) name="ferrodruid_plugin_hmac_bearer" ;;
    esac
    cp "$plugin/target/wasm32-unknown-unknown/release/${name}.wasm" "dist/${plugin}.wasm"
done

echo
echo "==> built artifacts"
ls -lh dist/

echo
echo "==> sha256 manifest"
for f in dist/*.wasm; do
    sha=$(sha256sum "$f" | awk '{print $1}')
    base=$(basename "$f")
    printf '%-32s %s\n' "$base" "$sha"
done
