#!/bin/bash
# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Wave 47-C — Real Druid 36.0.0 ⇄ FerroDruid wire/JSON diff harness.
#
# Mirrors `run_compat_v35.sh` but uses the
# `docker-compose.druid36.yml` stack on host port 36888.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.druid36.yml"
DRUID_PORT=36888
DRUID_BASE="http://localhost:${DRUID_PORT}"
TEST_NAME="druid_36_vs_ferrodruid_diff"

cd "$SCRIPT_DIR"

cleanup() {
    echo "Shutting down Druid 36 stack..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Apache Druid 36.0.0 (micro-quickstart) ==="
docker compose -f "$COMPOSE_FILE" up -d

echo "=== Waiting for Druid 36 to be healthy (up to 10 min on first pull / amd64-on-arm64 emulation) ==="
attempts=0
max_attempts=120
until curl -sf "${DRUID_BASE}/status/health" > /dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge "$max_attempts" ]; then
        echo "ERROR: Druid 36 did not become healthy after $max_attempts attempts"
        docker compose -f "$COMPOSE_FILE" logs druid | tail -80
        exit 1
    fi
    sleep 5
done
echo "Druid 36 is ready."

echo "=== Submitting batch ingestion task to Druid 36 ==="
TASK_ID=$(curl -sf -X POST "${DRUID_BASE}/druid/indexer/v1/task" \
    -H 'Content-Type: application/json' \
    -d @sample_ingestion_spec.json | python3 -c "import sys,json; print(json.load(sys.stdin)['task'])")
echo "Druid task ID: $TASK_ID"

echo "=== Waiting for Druid 36 ingestion to complete (up to 5 min) ==="
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

echo "=== Verifying data is queryable in Druid 36 ==="
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

echo "=== Running FerroDruid <-> Druid 36 diff test ==="
cd "$WORKSPACE_ROOT"
cargo test -p ferrodruid-rest --test druid_diff_test "$TEST_NAME" \
    -- --ignored --nocapture 2>&1 \
    | tee "$SCRIPT_DIR/RESULTS_wave47c_v36_run_stdout.txt"

echo "=== Done ==="
