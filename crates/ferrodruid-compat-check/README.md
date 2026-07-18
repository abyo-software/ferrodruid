<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# ferrodruid-compat-check (`ferro-compat-check`)

**Post-adoption verification tool.** Point it at a **running FerroDruid**
endpoint; it ingests three tiny built-in fixtures and runs ~40 SQL probes
whose expected values are lifted verbatim from live Apache Druid 30–36, then
reports PASS/FAIL. Use it to confirm a FerroDruid deployment behaves like
Druid on the surfaces you care about, and as a CI/upgrade regression check.

```
ferro-compat-check --url http://your-ferrodruid:8888 --section all
```

## This is NOT the log analyzer

There are two distinct tools — do not confuse them:

| Tool | Input | Runs anything? | When |
|---|---|---|---|
| **`ferro-compat-check`** (this crate) | a running FerroDruid URL | yes — executes probes | **after** adopting FerroDruid, to verify/regress it |
| **`ferro-logcompat`** (`crates/ferrodruid-logcompat`) | a customer's Druid **request log** | no — static parse+plan only | **before** adoption, to estimate compatibility from real query shapes without sharing data |

Exit codes: 0 compatible · 1 a probe failed · 2 setup error. `--json` for
machine output. Self-contained; no external services.
