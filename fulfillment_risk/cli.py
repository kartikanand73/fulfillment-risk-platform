"""python -m fulfillment_risk <command>

  init-db                 create/upgrade the fr schema and roles (admin connection)
  refresh [--synthetic]   run one refresh (Phase 1: synthetic sources only)
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

from .refresh import RefreshSettings, run_refresh
from .store import apply_schema, read_customers, read_lines, read_status


def _dsn() -> str:
    dsn = os.environ.get("FR_DATABASE_URL")
    if not dsn:
        sys.exit("set FR_DATABASE_URL")
    return dsn


def _print(rows):
    print(json.dumps(rows, indent=1, default=str))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="fulfillment_risk")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    r = sub.add_parser("refresh")
    r.add_argument("--synthetic", action="store_true", default=True)
    r.add_argument("--trigger", default="manual")
    sub.add_parser("status")
    s = sub.add_parser("show")
    s.add_argument("--at-risk", action="store_true")
    ln = sub.add_parser("lines")
    ln.add_argument("--customer")
    ln.add_argument("--order")
    a = ap.parse_args(argv)

    if a.cmd == "init-db":
        apply_schema(_dsn())
        print("schema fr ready")
    elif a.cmd == "refresh":
        from .sources.synthetic import synthetic_sources
        out = run_refresh(_dsn(), *synthetic_sources(), settings=RefreshSettings.from_env(), trigger=a.trigger)
        print(json.dumps({"run_id": out.run_id, "status": out.status, "error": out.error,
                          "summary": out.result.summary if out.result else None}, indent=1, default=str))
        return 0 if out.status == "succeeded" else 1
    elif a.cmd == "status":
        _print(read_status(_dsn()))
    elif a.cmd == "show":
        _print(read_customers(_dsn(), a.at_risk))
    elif a.cmd == "lines":
        _print(read_lines(_dsn(), a.customer, a.order))
    return 0


if __name__ == "__main__":
    sys.exit(main())
