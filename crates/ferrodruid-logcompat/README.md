# ferro-logcompat

ferro-logcompat statically classifies the queries in a Druid request log — it does NOT run anything and needs no data. (For live verification against a running FerroDruid, use `ferrodruid-compat-check` instead.)

Point it at an Apache Druid broker **file request log**
(`druid.request.logging.type=file`) and it answers one question, offline:
*how much of this Druid workload would FerroDruid accept?* Every logged
query is parsed through FerroDruid's own SQL parser + planner
(`parse_druid_sql` + `plan_sql`) or native-query deserializers — the exact
code the FerroDruid wire endpoints use — but **nothing is executed**: no
FerroDruid process, no Druid process, no segments, no network.

## Usage

```bash
# Markdown report to stdout
ferro-logcompat /path/to/broker/request-logs/2026-07-11.log

# JSON report to a file, top 50 incompatible shapes
ferro-logcompat 2026-07-11.log --json --top 50 --out report.json

# from a pipe
cat request-logs/*.log | ferro-logcompat --stdin
```

| flag | meaning |
|---|---|
| `<logfile>` | the Druid file request log to read |
| `--stdin` | read the log from standard input instead |
| `--json` | emit JSON (default: Markdown) |
| `--out <path>` | write the report to a file (default: stdout) |
| `--top <N>` | show the top N incompatible shapes (default: 20) |
| `--no-redact` | include each shape's first-seen query verbatim |

To produce the input on the Druid side, set in
`common.runtime.properties`:

```properties
druid.request.logging.type=file
druid.request.logging.dir=var/druid/request-logs
```

*Emitter*-format request logs (`druid.request.logging.type=emitter`) are
detected and reported as an unsupported input format instead of crashing;
use the file logger for now.

## What the report contains

* **Compatible %** — both shape-based (each distinct query shape counts
  once) and frequency-weighted (each log record counts).
* A **supported / fail-closed / unsupported** split:
  * `supported` — parses and plans through FerroDruid's existing query
    path (plan-through; results are not compared — that replay/diff step
    is Phase 2).
  * `fail-closed` — recognized constructs FerroDruid deliberately rejects
    (e.g. `FULL OUTER JOIN`, `WITH RECURSIVE`, JavaScript aggregators),
    with the rejection reason.
  * `unsupported` — parse/plan/deserialize errors, with the error.
* The **top-N incompatible shapes** by frequency, each with its reason.

Queries are first *shape-normalized*: literal values (filter constants,
interval bounds, `LIMIT`/threshold numbers) are stripped to `?` and
identical shapes are grouped and counted, so the report is weighted by
what the workload actually does, not by how many literal variants it has.

Two kinds of log records are set aside (listed, but excluded from the
percentages) because FerroDruid never receives them on the wire:

* **segment-pinned fan-out sub-queries** the broker sends to data nodes
  (`intervals: {"type":"segments", …}`) — cluster-internal machinery;
* **Calcite lowerings of SQL requests** (natives whose context carries
  `sqlQueryId`) — the same workload is already counted by its SQL log
  line, and FerroDruid plans SQL with its own planner.

## Privacy design (default-on redaction)

This tool is built to be run by *you*, on *your* logs, on *your* machine,
with only the report leaving the building:

* **Local only, no exfiltration** — the binary performs no network I/O of
  any kind; it reads the log file (or stdin) and writes the report.
* **No data access** — it never connects to Druid or FerroDruid and never
  reads segments; there is no table data anywhere in the pipeline.
* **No literals in the report** — the default report contains only
  literal-stripped query shapes and counts. Filter constants, interval
  bounds, limits and string/numeric literals are all masked to `?` before
  anything is grouped or printed. SQL comments (`--` to end of line and
  `/* … */` blocks) are stripped entirely — comment text is customer
  text — and string literals are scanned under both quote conventions
  (SQL `''` doubling and the backslash escapes of Druid/Calcite
  expression strings) in parallel, masking anything either reading
  considers string content, so a mis-guessed convention can only
  over-mask, never leak. Native-query masking is default-deny **by
  path**: a string survives only when its exact position in the query
  tree is a known structural slot (a key name alone never whitelists a
  value), data-keyed maps (lookup maps, paging identifiers) are masked
  keys-and-values, and everything under an unanticipated key is masked.
  Error messages echoed into `reason` fields pass through the same
  literal masking. Structural text — table names, column names, function
  names — is retained, since the report is useless without it.
* **Bounded memory on hostile input** — lines longer than 8 MiB are
  skipped unread (their bytes are discarded as they stream past), counted
  in the report as `oversized lines`, and never parsed, so a pathological
  multi-hundred-MiB log line cannot drive the tool into out-of-memory.
* `--no-redact` (opt-in) additionally includes each shape's first-seen
  query text verbatim (including its literals) for local debugging. Even
  then the tool only ever emits query text — never any table data,
  because it reads none.

## Relationship to `ferrodruid-compat-check`

They are complementary and deliberately separate:

| | `ferro-logcompat` (this crate) | `ferro-compat-check` |
|---|---|---|
| input | a Druid request log file | a **running** FerroDruid URL |
| executes queries | never | yes (probe battery) |
| needs data | no | yes (loads its own fixtures) |
| answers | "how much of *your* workload parses + plans?" | "does *this* FerroDruid behave Druid-correctly?" |

## Phase 2 (not built)

Replay-diff — re-executing the supported shapes against a FerroDruid and
byte-comparing results with Druid — is Phase 2 and out of scope here.
