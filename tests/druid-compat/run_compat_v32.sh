#!/bin/bash
# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Wave 47-B — Real Druid 32.0.1 ⇄ FerroDruid wire/JSON diff harness.
#
# Mirrors `run_compat.sh` (which targets Druid 30.0.1) but uses the
# `docker-compose.druid32.yml` stack on host port 18888.
#
# This script:
#   1. Starts Apache Druid 32.0.1 via Docker Compose (single container,
#      micro-quickstart launcher — bundled Derby + ZooKeeper).
#   2. Waits for the Druid router to become healthy.
#   3. Submits the inline `wikipedia_compat` ingestion spec.
#   4. Waits for the ingestion task to SUCCEED.
#   5. Builds the FerroDruid release binary (idempotent).
#   6. Runs `cargo test druid_32_vs_ferrodruid_diff -- --ignored` which
#      itself spawns a fresh FerroDruid subprocess on port 38889,
#      submits the same spec to FerroDruid, runs SQL queries against
#      both engines, and writes per-query diffs to RESULTS_wave47b_v32_run.md.
#   7. Tears down the Druid stack on exit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.druid32.yml"
DRUID_PORT=18888
DRUID_BASE="http://localhost:${DRUID_PORT}"
TEST_NAME="druid_32_vs_ferrodruid_diff"

cd "$SCRIPT_DIR"

cleanup() {
    echo "Shutting down Druid 32 stack..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Apache Druid 32.0.1 (micro-quickstart) ==="
docker compose -f "$COMPOSE_FILE" up -d

echo "=== Waiting for Druid 32 to be healthy (up to 10 min on first pull / amd64-on-arm64 emulation) ==="
attempts=0
max_attempts=120
until curl -sf "${DRUID_BASE}/status/health" > /dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Druid 32 did not become healthy after $max_attempts attempts"
        docker compose -f "$COMPOSE_FILE" logs druid | tail -80
        exit 1
    fi
    sleep 5
done
echo "Druid 32 is ready."

echo "=== Submitting batch ingestion task to Druid 32 ==="
TASK_ID=$(curl -sf -X POST "${DRUID_BASE}/druid/indexer/v1/task" \
    -H 'Content-Type: application/json' \
    -d @sample_ingestion_spec.json | python3 -c "import sys,json; print(json.load(sys.stdin)['task'])")
echo "Druid task ID: $TASK_ID"

echo "=== Waiting for Druid 32 ingestion to complete (up to 5 min) ==="
attempts=0
max_attempts=60
while true; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Ingestion did not complete after $max_attempts attempts"
        exit 1
    fi
    STATUS=$(curl -sf "${DRUID_BASE}/druid/indexer/v1/task/$TASK_ID/status" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['status']['status'])" 2>/dev/null || echo "UNKNOWN")
    echo "  Druid task status: $STATUS"
    if [ "$STATUS" = "SUCCESS" ]; then
        break
    elif [ "$STATUS" = "FAILED" ]; then
        echo "ERROR: Druid ingestion task failed"
        curl -sf "${DRUID_BASE}/druid/indexer/v1/task/$TASK_ID/status" | python3 -m json.tool
        exit 1
    fi
    sleep 5
done

echo "=== Verifying data is queryable in Druid 32 ==="
for i in $(seq 1 30); do
    RESULT=$(curl -sf -X POST "${DRUID_BASE}/druid/v2/sql" \
        -H 'Content-Type: application/json' \
        -d '{"query": "SELECT COUNT(*) AS cnt FROM wikipedia_compat"}' 2>/dev/null || echo '[]')
    echo "  attempt $i: $RESULT"
    CNT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0].get('cnt',0) if d else 0)" 2>/dev/null || echo 0)
    if [ "$CNT" -gt 0 ]; then
        echo "Druid datasource has $CNT rows."
        break
    fi
    sleep 2
done

echo "=== Building FerroDruid release binary (idempotent) ==="
cd "$WORKSPACE_ROOT"
cargo build --release -p ferrodruid-cli-lib

echo "=== Running FerroDruid <-> Druid 32 diff test ==="
cd "$WORKSPACE_ROOT"
cargo test -p ferrodruid-rest --test druid_diff_test "$TEST_NAME" \
    -- --ignored --nocapture 2>&1 \
    | tee "$SCRIPT_DIR/RESULTS_wave47b_v32_run_stdout.txt"

echo "=== Done ==="
