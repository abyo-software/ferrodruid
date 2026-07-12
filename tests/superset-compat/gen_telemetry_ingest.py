#!/usr/bin/env python3
# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
"""Generate the `telemetry` inline ingest spec used by the Superset ⇄ FerroDruid
UI-evidence re-shoot (Superset E2E screenshots).

Dataset shape (matches the S-3 README): `__time` / `site_id` / `device_id` /
`metric_name` / `value` / `status`; 8 sites x 3 metrics x 48 hourly steps =
1,152 rows, spanning **two** UTC days so a `segmentGranularity=DAY` ingest lands
**two segments**. That is deliberate: the multi-segment post-aggregation merge
path (the bug fixed for v1.1.1) only runs when a bucket holds >=2 segments, so a
cross-day `AVG(value)` in these screenshots is a live regression check that the
v1.1.1+ binary merges partials correctly (v1.0.0 / v1.1.0 returned the first
segment's value).

Deterministic (no RNG) so the fixture — and therefore every screenshot's
numbers — is byte-reproducible. Emits the full index_parallel spec as JSON on
stdout.
"""
import argparse
import json
import math
import sys

SITES = [f"site_{i:02d}" for i in range(1, 9)]  # site_01 .. site_08
METRICS = [
    # (name, base, amplitude, unit-ish spread) — diurnal sine so the line
    # chart has an obvious shape and per-site means are all distinct.
    ("power_kw", 40.0, 30.0),
    ("voltage_v", 230.0, 8.0),
    ("temperature_c", 22.0, 6.0),
]
STEPS = 48                     # hourly steps
START_EPOCH_H = 0             # 2026-03-01T00:00:00Z, hour 0
BASE_DAY = "2026-03-01"       # day 1; hour>=24 rolls into 2026-03-02 (day 2)


def iso_for_hour(h: int) -> str:
    day = 1 + (h // 24)
    hour = h % 24
    return f"2026-03-{day:02d}T{hour:02d}:00:00Z"


def value_for(site_idx: int, metric_idx: int, base: float, amp: float, h: int) -> float:
    # Per-site phase + offset so the 8 site lines are visually separable and
    # every site's daily mean differs (good for the AVG bar chart ordering).
    phase = (site_idx * math.pi) / 8.0
    site_offset = site_idx * (1.0 + metric_idx)  # distinct per (site, metric)
    diurnal = amp * math.sin(2.0 * math.pi * (h % 24) / 24.0 + phase)
    # Per-DAY ramp: day-2 readings sit a fixed step above day-1. This makes the
    # two DAY segments hold *different* per-site means, so a cross-day AVG
    # (segment-1 mean != segment-2 mean != merged mean) is a genuine regression
    # check on the v1.1.1 multi-segment post-agg merge fix. Without it, both
    # segments would carry identical means and the pre-fix (first-segment-only)
    # value would be indistinguishable from the correct merged value.
    day_term = (h // 24) * (amp * 0.5)
    v = base + site_offset + diurnal + day_term
    return round(v, 3)


def status_for(h: int, site_idx: int) -> str:
    # Mostly ok; a deterministic sprinkle of warn/critical for the status dim.
    m = (h + site_idx) % 17
    if m == 0:
        return "critical"
    if m in (3, 11):
        return "warn"
    return "ok"


def main() -> int:
    # Optional day selector: `--day 1` emits hours 0..23 (UTC day 1), `--day 2`
    # emits hours 24..47 (UTC day 2). Ingesting the two days as two separate
    # tasks lands two disjoint DAY segments in the same datasource, so a
    # cross-day query genuinely merges two segments (the multi-seg post-agg
    # path). With no --day, emits all 48 hours (single-task convenience).
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--day", type=int, choices=(1, 2), default=None,
        help="emit only UTC day 1 (hours 0..23) or day 2 (hours 24..47); "
             "omit to emit all 48 hours",
    )
    only_day = parser.parse_args().day
    rows = []
    for h in range(STEPS):
        if only_day == 1 and h >= 24:
            continue
        if only_day == 2 and h < 24:
            continue
        ts = iso_for_hour(h)
        for s_idx, site in enumerate(SITES):
            for m_idx, (mname, base, amp) in enumerate(METRICS):
                rows.append({
                    "timestamp": ts,
                    "site_id": site,
                    "device_id": f"{site}-inv-01",
                    "metric_name": mname,
                    "value": value_for(s_idx, m_idx, base, amp, h),
                    "status": status_for(h, s_idx),
                })
    data = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)

    spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "telemetry",
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {
                    "dimensions": ["site_id", "device_id", "metric_name", "status"]
                },
                "metricsSpec": [
                    {"type": "count", "name": "count"},
                    {"type": "doubleSum", "name": "value", "fieldName": "value"},
                ],
                "granularitySpec": {
                    "type": "uniform",
                    # DAY segment granularity over a 2-day span => 2 segments,
                    # exercising the multi-segment post-agg merge on cross-day
                    # AVG/SUM. queryGranularity NONE keeps every hourly point.
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": data},
                "inputFormat": {"type": "json"},
            },
        },
    }
    json.dump(spec, sys.stdout)
    print(f"# rows={len(rows)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
