"""python -m fulfillment_risk <command>

  init-db                 create/upgrade the fr schema and roles (admin connection)
  refresh [--live] [--dry-run]   run one refresh (synthetic sources unless --live; --dry-run skips PostgreSQL)
  check-sources [--live]  read each source once and print row counts + freshness (no calculation, no DB)
  status                  published snapshot: time, source freshness, failures since
  show [--at-risk]        customer results from the published snapshot
  lines [--customer ID] [--order ID]

Connection: FR_DATABASE_URL (e.g. postgresql://fr_refresh@localhost:5432/fr). Exit code 1 if a refresh fails.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from dataclasses import asdict

from .refresh import RefreshSettings, dry_run, run_refresh
from .store import apply_schema, read_customers, read_lines, read_status


def load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader (KEY=VALUE lines); real environment variables win."""
    if not os.path.exists(path):
        return
    for raw in open(path, encoding="utf-8"):
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _dsn() -> str:
    dsn = os.environ.get("FR_DATABASE_URL")
    if not dsn:
        sys.exit("set FR_DATABASE_URL")
    return dsn


def _sources(live: bool):
    if live:
        from .sources.live import live_sources
        return live_sources()
    from .sources.synthetic import synthetic_sources
    return synthetic_sources()


def _print(rows):
    print(json.dumps(rows, indent=1, default=str))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="fulfillment_risk")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    r = sub.add_parser("refresh")
    r.add_argument("--live", action="store_true", help="real Salesforce / Databricks / Snowflake")
    r.add_argument("--dry-run", action="store_true", help="compute and print; do not touch PostgreSQL")
    r.add_argument("--trigger", default="manual")
    cs = sub.add_parser("check-sources")
    cs.add_argument("--live", action="store_true")
    sub.add_parser("status")
    s = sub.add_parser("show")
    s.add_argument("--at-risk", action="store_true")
    ln = sub.add_parser("lines")
    ln.add_argument("--customer")
    ln.add_argument("--order")
    a = ap.parse_args(argv)
    load_dotenv()

    if a.cmd == "init-db":
        apply_schema(_dsn())
        print("schema fr ready")
    elif a.cmd == "refresh":
        srcs = _sources(a.live)
        if a.dry_run:
            out = dry_run(*srcs, settings=RefreshSettings.from_env())
        else:
            out = run_refresh(_dsn(), *srcs, settings=RefreshSettings.from_env(), trigger=a.trigger)
        report = {"run_id": out.run_id, "status": out.status, "error": out.error, "sources": out.sources,
                  "summary": out.result.summary if out.result else None}
        if a.dry_run and out.result:
            report["customers"] = [asdict(c) for c in out.result.customers]
            report["lines_at_risk"] = [asdict(l) for l in out.result.lines if l.status.value != "FULFILLABLE"]
        print(json.dumps(report, indent=1, default=str))
        return 0 if out.status == "succeeded" else 1
    elif a.cmd == "check-sources":
        rc = 0
        for src, fn in zip(_sources(a.live), ("fetch_customers", "fetch_open_order_lines", "fetch_inventory")):
            try:
                b = getattr(src, fn)()
                print(f"OK   {src.name:22} rows={b.row_count:<5} as_of={b.as_of.isoformat()}")
            except Exception as e:
                rc = 1
                print(f"FAIL {src.name:22} {e}")
        return rc
    elif a.cmd == "status":
        _print(read_status(_dsn()))
    elif a.cmd == "show":
        _print(read_customers(_dsn(), a.at_risk))
    elif a.cmd == "lines":
        _print(read_lines(_dsn(), a.customer, a.order))
    return 0


if __name__ == "__main__":
    sys.exit(main())
