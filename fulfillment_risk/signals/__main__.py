"""python -m fulfillment_risk.signals publish --scenario all [--tag TAG] [--dry-run]"""
import argparse
import json
import sys
import uuid

from ..cli import load_dotenv
from .publisher import build, publish


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="fulfillment_risk.signals")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("publish")
    p.add_argument("--scenario", default="normal",
                   choices=["normal", "duplicates", "out-of-order", "invalid", "burst", "all"])
    p.add_argument("--tag", default=None)
    p.add_argument("--burst", type=int, default=20)
    p.add_argument("--dry-run", action="store_true", help="print what would be sent")
    a = ap.parse_args(argv)
    load_dotenv()
    tag = a.tag or uuid.uuid4().hex[:8]
    m = build(a.scenario, tag, burst=a.burst)
    if a.dry_run:
        for i, g in enumerate(m.sends):
            for x in g:
                print(i, x.expect, x.partition_key, x.body[:120])
    else:
        n = publish(m)
        print(f"sent {n} event(s)")
    print(json.dumps({"tag": tag, "expected": m.expected()}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
