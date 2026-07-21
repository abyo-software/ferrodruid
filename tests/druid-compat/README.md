# Druid Compatibility Tests

Run FerroDruid against real Apache Druid instances
(30.0.1 / 31.0.2 / 32.0.1 / 33.0.0 / 34.0.0 / 35.0.1 / 36.0.0) to verify
wire-level and metadata compatibility.

## Prerequisites

- Docker and Docker Compose
- ~4 GB free RAM per Druid version (micro-quickstart all-in-one)
- Per-version host port ranges (see Endpoints below)

## Quick start

```bash
# Druid 30.0.1 (Wave 30, original baseline)
./run_compat.sh

# Druid 31.0.2 (Wave 47-C)
./run_compat_v31.sh

# Druid 32.0.1 (Wave 47-B)
./run_compat_v32.sh

# Druid 33.0.0 (Wave 47-C)
./run_compat_v33.sh

# Druid 34.0.0 (Wave 47-C)
./run_compat_v34.sh

# Druid 35.0.1 (Wave 47-B)
./run_compat_v35.sh

# Druid 36.0.0 (Wave 47-C, latest upstream stable)
./run_compat_v36.sh
```

Each script:

1. Starts the per-version Druid container (single container, bundled
   Derby + ZooKeeper + all 6 services via `start-micro-quickstart`).
2. Waits for Druid to become healthy (~60-120 s on first pull).
3. Submits a batch ingestion with inline sample data (wikipedia-like).
4. Waits for the ingestion task to succeed.
5. Verifies the data is queryable via Druid SQL.
6. Runs the matching `druid_NN_vs_ferrodruid_diff` Rust test
   (compares FerroDruid query results against Druid).
7. Tears down the Docker stack on exit.

Wave 47-D expanded each test to 21 queries / version across four
sections:

| Section                       | Queries | Examples                                              |
|-------------------------------|---------|-------------------------------------------------------|
| 1. Base SQL surface           | 5       | `count_star`, `min_max_added`, `groupby_page_topn`    |
| 2. SQL window functions       | 8       | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `SUM/AVG OVER` |
| 3. Native TIMESERIES (POST `/druid/v2`) | 4 | `count by day`, `multi-agg`, hour granularity     |
| 4. Native TopN (POST `/druid/v2`)       | 4 | `top 5 page by count`, `top 5 user by sum`, ascending min |

See `docs/compatibility-matrix.md` (section "Wave 47-D divergences")
for the documented mismatches surfaced by sections 2-4.

## Cargo test entry points

| Test fn                           | Druid version | Druid host port | FerroDruid port |
|-----------------------------------|---------------|-----------------|-----------------|
| `druid_30_vs_ferrodruid_diff`     | 30.0.1        | 8888            | 38888           |
| `druid_32_vs_ferrodruid_diff`     | 32.0.1        | 18888           | 38889           |
| `druid_35_vs_ferrodruid_diff`     | 35.0.1        | 28888           | 38890           |
| `druid_36_vs_ferrodruid_diff`     | 36.0.0        | 36888           | 38891           |
| `druid_33_vs_ferrodruid_diff`     | 33.0.0        | 33888           | 38893           |
| `druid_34_vs_ferrodruid_diff`     | 34.0.0        | 34888           | 38894           |
| `druid_31_vs_ferrodruid_diff`     | 31.0.2        | 31888           | 38895           |

All three are `#[ignore]` by default; they SKIP gracefully if the
matching Druid container is not reachable.

```bash
cargo test -p ferrodruid-rest --test druid_diff_test \
    -- --ignored --nocapture
```

## Manual operation

```bash
# Druid 30 (default)
docker compose up -d
curl http://localhost:8888/status/health

# Druid 31
docker compose -f docker-compose.druid31.yml up -d
curl http://localhost:31888/status/health

# Druid 32
docker compose -f docker-compose.druid32.yml up -d
curl http://localhost:18888/status/health

# Druid 33
docker compose -f docker-compose.druid33.yml up -d
curl http://localhost:33888/status/health

# Druid 34
docker compose -f docker-compose.druid34.yml up -d
curl http://localhost:34888/status/health

# Druid 35
docker compose -f docker-compose.druid35.yml up -d
curl http://localhost:28888/status/health

# Druid 36
docker compose -f docker-compose.druid36.yml up -d
curl http://localhost:36888/status/health

# Submit ingestion (replace PORT with the Druid you started)
curl -X POST http://localhost:PORT/druid/indexer/v1/task \
  -H 'Content-Type: application/json' \
  -d @sample_ingestion_spec.json

# Query via SQL
curl -X POST http://localhost:PORT/druid/v2/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM wikipedia_compat LIMIT 5"}'

# Tear down
docker compose -f <compose-file> down -v
```

## Endpoints

| Service       | v30 port | v31 port | v32 port | v33 port | v34 port | v35 port | v36 port |
|---------------|----------|----------|----------|----------|----------|----------|----------|
| Router (web)  | 8888     | 31888    | 18888    | 33888    | 34888    | 28888    | 36888    |
| Coordinator   | 8081     | 31081    | 18081    | 33081    | 34081    | 28081    | 36081    |
| Broker        | 8082     | 31082    | 18082    | 33082    | 34082    | 28082    | 36082    |
| Historical    | 8083     | 31083    | 18083    | 33083    | 34083    | 28083    | 36083    |
| Overlord      | 8090     | 31090    | 18090    | 33090    | 34090    | 28090    | 36090    |
| MiddleManager | 8091     | 31091    | 18091    | 33091    | 34091    | 28091    | 36091    |

## Image building

The official `apache/druid:*` images are distroless and ship without
perl, which `start-micro-quickstart` requires.  We build a thin Debian
re-base via `Dockerfile.druid.template`, parametrized by `DRUID_VERSION`
build arg.  Each compose file sets the build arg, so you only need
`docker compose up -d` — the image is built on first invocation.
