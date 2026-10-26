"""Live acceptance test for operational signals (run with `az login`; you need Data Sender on the hub and to be the
PostgreSQL Entra admin). Every event is tagged (source = acceptance-<tag>) so runs never interfere.

  A1  publish 'all' (normal + duplicates + out-of-order + invalid) and a 10-event burst through Event Hubs
  A2  PostgreSQL has exactly the expected accepted / duplicate / late / rejected counts for this tag
  A3  each rejected event has the right reason
  A4  the fulfillment-risk snapshot is unchanged (context only)
  A5  read-only views return the events (v_signal_summary / v_signal_recent / v_signal_rejected_recent)
  A6  App Insights has the Function's fr_signals_batch lines and no failed invocations for this window
usage: python deploy/azure/acceptance_signals.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))
from provision import AZ, DB, pg_connect  # noqa: E402
from signals import FUNC, HUB, NS, admin_name  # noqa: E402

from fulfillment_risk.signals.publisher import Manifest, build, publish  # noqa: E402

results: list[tuple[str, bool, str]] = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def token():
    return subprocess.run([AZ, "account", "get-access-token", "--resource-type", "oss-rdbms", "--query",
                           "accessToken", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()


def snapshot(c):
    run = c.execute("SELECT run_id FROM fr.published_snapshot").fetchone()
    rows = c.execute("SELECT customer_id, status, short_value FROM fr.customer_risk WHERE run_id = %s ORDER BY 1",
                     (run[0],)).fetchall() if run else []
    return (run[0] if run else None), rows


def counts(c, tag):
    src = f"acceptance-{tag}"
    a, d, late = c.execute("SELECT count(*), coalesce(sum(duplicate_count),0), count(*) FILTER (WHERE is_late) "
                           "FROM fr.signal_event WHERE source = %s", (src,)).fetchone()
    rej = c.execute("SELECT reason FROM fr.signal_rejected WHERE raw LIKE %s ORDER BY id", (f"%{tag}%",)).fetchall()
    return {"accepted": a, "duplicate": int(d), "late": late, "rejected": len(rej)}, [r[0] for r in rej]


def appi(query: str) -> list:
    app_id = subprocess.run([AZ, "monitor", "app-insights", "component", "show", "-g", "DB", "-a",
                             "appi-skillagent-ka2026", "--query", "appId", "-o", "tsv"],
                            capture_output=True, text=True, check=True).stdout.strip()
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump({"query": query}, f)
    try:
        r = subprocess.run([AZ, "rest", "--method", "post", "--resource", "https://api.applicationinsights.io",
                            "--url", f"https://api.applicationinsights.io/v1/apps/{app_id}/query", "--body", f"@{path}"],
                           capture_output=True, text=True)
    finally:
        os.remove(path)
    return json.loads(r.stdout)["tables"][0]["rows"] if r.returncode == 0 else []


def main() -> int:
    tag = uuid.uuid4().hex[:8]
    t0 = datetime.now(timezone.utc)
    print(f"acceptance run tag={tag} start={t0:%H:%M:%S} UTC")
    adm, tok = admin_name(), token()
    with pg_connect(DB, adm, tok) as c:
        run_before, snap_before = snapshot(c)

    m_all, m_burst = build("all", tag), build("burst", tag, burst=10)
    m = Manifest(tag, m_all.sends + m_burst.sends)
    exp = m.expected()
    sent = publish(m, f"{NS}.servicebus.windows.net", HUB)
    check("A1 published to Event Hubs (Entra auth, no keys)", sent == sum(len(g) for g in m.sends), f"{sent} sends")

    got, reasons = {}, []
    for i in range(36):                                  # up to 3 minutes (Flex cold start + batching)
        time.sleep(5)
        with pg_connect(DB, adm, tok) as c:
            got, reasons = counts(c, tag)
        if got == exp:
            break
        if i % 4 == 3:
            print(f"   {(i + 1) * 5}s: {got} (want {exp})", flush=True)
    check("A2 stored counts match (accepted / duplicate / late / rejected)", got == exp, f"got {got} want {exp}")
    check("A3 rejection reasons", sorted(reasons) == sorted(
        ["bad_json", "unknown_type", "future_event", "bad_field", "missing_field"]), ", ".join(reasons))

    with pg_connect(DB, adm, tok) as c:
        run_after, snap_after = snapshot(c)
        same = snap_after == snap_before
        check("A4 fulfillment-risk snapshot unchanged by events", same,
              f"run {run_before} -> {run_after}" + ("" if run_before == run_after else " (hourly refresh ran; values compared)"))
        summ = c.execute("SELECT accepted_1h, rejected_1h, duplicates_1h, late_1h FROM fr.v_signal_summary").fetchone()
        recent = c.execute("SELECT count(*), count(snapshot_order_status), count(*) FILTER (WHERE after_snapshot) "
                           "FROM fr.v_signal_recent WHERE source = %s", (f"acceptance-{tag}",)).fetchone()
        rej_v = c.execute("SELECT count(*) FROM fr.v_signal_rejected_recent WHERE raw_preview LIKE %s",
                          (f"%{tag}%",)).fetchone()[0]
    check("A5 read-only views", summ[0] >= exp["accepted"] and recent[0] == exp["accepted"] and rej_v == exp["rejected"],
          f"summary(1h)={summ} recent={recent[0]} with snapshot status={recent[1]} rejected view={rej_v}")

    since = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    batches, fails = [], [[None]]
    for _ in range(18):                                  # App Insights ingestion: allow up to ~3 more minutes
        batches = appi(f"traces | where timestamp > datetime({since}) and cloud_RoleName == '{FUNC}' "
                       "and message startswith 'fr_signals_batch' | project message")
        fails = appi(f"requests | where timestamp > datetime({since}) and cloud_RoleName == '{FUNC}' "
                     "and success == false | count")
        tot = sum(int(r[0].split("accepted=")[1].split()[0]) for r in batches)
        if tot >= exp["accepted"]:
            break
        time.sleep(10)
    check("A6 telemetry in App Insights", bool(batches) and (fails[0][0] if fails else 0) == 0,
          f"{len(batches)} batch lines, accepted total {sum(int(r[0].split('accepted=')[1].split()[0]) for r in batches)}, "
          f"failed invocations {fails[0][0] if fails else '?'}")
    ok = all(r[1] for r in results)
    print(f"\nRESULT: {'ALL PASSED' if ok else 'FAILED'} ({sum(r[1] for r in results)}/{len(results)})  tag={tag}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
