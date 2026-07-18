#!/usr/bin/env python3
# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
"""S-1 pydruid smoke test — exercise every pydruid access path Apache Superset
uses against a running FerroDruid single-binary, and emit a pass/fail matrix.

Superset reaches Druid three ways, all covered here:

  * DBAPI (`pydruid.db.connect`) — SQL Lab / raw query execution.
  * SQLAlchemy dialect (`druid://...`) — engine, connection ping, schema
    introspection (`get_table_names`, `get_columns`) used for dataset sync.
  * PyDruid native client (`pydruid.client.PyDruid`) — legacy native
    timeseries / topN / groupBy against `/druid/v2`.

Usage:  python3 pydruid_smoke.py --host 127.0.0.1 --port 38899 \
              --datasource wikipedia_compat [--json out.json]

Exit code is 0 always (the point is to record the matrix, not to gate);
callers read the JSON / the printed table.
"""
import argparse
import json
import sys
import traceback


def _result(ok, detail):
    return {"pass": bool(ok), "detail": str(detail)[:400]}


def run_dbapi(host, port, datasource):
    """DBAPI: connect -> cursor -> execute -> fetchall."""
    out = {}
    from pydruid.db import connect

    conn = connect(host=host, port=port, path="/druid/v2/sql/", scheme="http")
    # do_ping-style constant SELECT (FerroDruid FROM-less constant SELECT).
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        rows = cur.fetchall()
        out["dbapi_select_1"] = _result(len(rows) == 1, rows)
    except Exception as e:  # noqa: BLE001
        out["dbapi_select_1"] = _result(False, f"{type(e).__name__}: {e}")

    # COUNT(*) over the ingested datasource.
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) AS cnt FROM "{datasource}"')
        rows = cur.fetchall()
        cnt = rows[0][0] if rows else None
        out["dbapi_count"] = _result(cnt and cnt > 0, f"count={cnt}")
    except Exception as e:  # noqa: BLE001
        out["dbapi_count"] = _result(False, f"{type(e).__name__}: {e}")

    # A GROUP BY (Superset chart shape).
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT "language", COUNT(*) AS c FROM "{datasource}" '
            f'GROUP BY "language" ORDER BY c DESC'
        )
        rows = cur.fetchall()
        out["dbapi_group_by"] = _result(len(rows) > 0, f"{len(rows)} groups: {rows[:3]}")
    except Exception as e:  # noqa: BLE001
        out["dbapi_group_by"] = _result(False, f"{type(e).__name__}: {e}")
    return out


def run_sqlalchemy(host, port, datasource):
    """SQLAlchemy dialect: engine, ping, introspection, query."""
    out = {}
    from sqlalchemy import create_engine, inspect, text

    uri = f"druid://{host}:{port}/druid/v2/sql/"
    engine = create_engine(uri)

    # Connection / do_ping.
    try:
        with engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
        out["sa_connect_ping"] = _result(val == 1, f"SELECT 1 -> {val}")
    except Exception as e:  # noqa: BLE001
        out["sa_connect_ping"] = _result(False, f"{type(e).__name__}: {e}")

    # get_table_names (dataset picker).
    try:
        names = inspect(engine).get_table_names()
        out["sa_get_table_names"] = _result(
            datasource in names, f"tables={names}"
        )
    except Exception as e:  # noqa: BLE001
        out["sa_get_table_names"] = _result(False, f"{type(e).__name__}: {e}")

    # get_columns (dataset column sync).
    try:
        cols = inspect(engine).get_columns(datasource)
        names = [c.get("name") for c in cols]
        out["sa_get_columns"] = _result(len(cols) > 0, f"columns={names}")
    except Exception as e:  # noqa: BLE001
        out["sa_get_columns"] = _result(False, f"{type(e).__name__}: {e}")

    # SELECT through the dialect.
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(f'SELECT COUNT(*) AS c FROM "{datasource}"')
            ).fetchall()
        out["sa_select"] = _result(rows and rows[0][0] > 0, f"{rows}")
    except Exception as e:  # noqa: BLE001
        out["sa_select"] = _result(False, f"{type(e).__name__}: {e}")
    return out


def run_native(host, port, datasource):
    """PyDruid native client: timeseries / topN / groupBy against /druid/v2."""
    out = {}
    from pydruid.client import PyDruid
    from pydruid.utils.aggregators import longsum

    client = PyDruid(f"http://{host}:{port}", "druid/v2")
    interval = "2024-01-01T00:00:00/2024-01-05T00:00:00"

    try:
        ts = client.timeseries(
            datasource=datasource,
            granularity="all",
            intervals=interval,
            aggregations={"total_added": longsum("added")},
        )
        rows = ts.result
        out["native_timeseries"] = _result(len(rows) >= 1, f"{rows}")
    except Exception as e:  # noqa: BLE001
        out["native_timeseries"] = _result(False, f"{type(e).__name__}: {e}")

    try:
        tn = client.topn(
            datasource=datasource,
            granularity="all",
            intervals=interval,
            aggregations={"total_added": longsum("added")},
            dimension="language",
            metric="total_added",
            threshold=5,
        )
        rows = tn.result
        out["native_topn"] = _result(len(rows) >= 1, f"{rows}")
    except Exception as e:  # noqa: BLE001
        out["native_topn"] = _result(False, f"{type(e).__name__}: {e}")

    try:
        gb = client.groupby(
            datasource=datasource,
            granularity="all",
            intervals=interval,
            dimensions=["language"],
            aggregations={"total_added": longsum("added")},
        )
        rows = gb.result
        out["native_groupby"] = _result(len(rows) >= 1, f"{rows}")
    except Exception as e:  # noqa: BLE001
        out["native_groupby"] = _result(False, f"{type(e).__name__}: {e}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=38899)
    ap.add_argument("--datasource", default="wikipedia_compat")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    try:
        import pydruid

        pydruid_ver = getattr(pydruid, "__version__", "unknown")
    except Exception:  # noqa: BLE001
        pydruid_ver = "unknown"
    try:
        from importlib.metadata import version

        pydruid_ver = version("pydruid")
    except Exception:  # noqa: BLE001
        pass

    matrix = {"pydruid_version": pydruid_ver, "surfaces": {}}
    for name, fn in (
        ("dbapi", run_dbapi),
        ("sqlalchemy", run_sqlalchemy),
        ("native", run_native),
    ):
        try:
            matrix["surfaces"][name] = fn(args.host, args.port, args.datasource)
        except Exception as e:  # noqa: BLE001
            matrix["surfaces"][name] = {
                "_surface_error": _result(False, traceback.format_exc())
            }

    # Print a compact table.
    print(f"\n=== pydruid {pydruid_ver} vs FerroDruid ({args.host}:{args.port}) ===")
    total = passed = 0
    for surface, checks in matrix["surfaces"].items():
        for check, res in checks.items():
            total += 1
            passed += 1 if res["pass"] else 0
            mark = "PASS" if res["pass"] else "FAIL"
            print(f"  [{mark}] {surface}.{check}: {res['detail']}")
    print(f"--- {passed}/{total} passed ---")
    matrix["passed"] = passed
    matrix["total"] = total

    if args.json:
        with open(args.json, "w") as f:
            json.dump(matrix, f, indent=2)
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
