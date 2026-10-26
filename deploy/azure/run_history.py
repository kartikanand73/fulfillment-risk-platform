"""Run history from PostgreSQL (as you, Entra) + the job's console output from Log Analytics."""
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
AZ = "az.cmd" if sys.platform == "win32" else "az"
os.environ["FR_DATABASE_ENTRA"] = "true"
upn = subprocess.run([AZ, "ad", "signed-in-user", "show", "--query", "userPrincipalName", "-o", "tsv"],
                     capture_output=True, text=True, check=True).stdout.strip()
from fulfillment_risk.store.postgres import connect  # noqa: E402

with connect(f"host=psql-fr-ka2026.postgres.database.azure.com dbname=fr user={upn}") as c:
    print("PostgreSQL fr.refresh_run:")
    for r in c.execute("SELECT run_id, status, trigger, code_version, started_at, finished_at - started_at, "
                       "summary->>'customers_at_risk', summary->>'short_value', left(error, 80) "
                       "FROM fr.refresh_run ORDER BY run_id"):
        print("  ", " | ".join(str(x) for x in r))
    print("published run:", c.execute("SELECT run_id FROM fr.published_snapshot").fetchone())

ws = subprocess.run([AZ, "monitor", "log-analytics", "workspace", "show", "-g", "DB", "-n", "log-skillagent-ka2026",
                     "--query", "customerId", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()
q = """ContainerAppConsoleLogs_CL
| where ContainerJobName_s == 'job-fr-refresh' and Log_s has '"status"'
| project TimeGenerated, ContainerGroupName_s, Log_s | order by TimeGenerated desc | take 10"""
fd, path = tempfile.mkstemp(suffix=".json")
with os.fdopen(fd, "w") as f:
    json.dump({"query": q, "timespan": "PT3H"}, f)
try:
    r = subprocess.run([AZ, "rest", "--method", "post", "--resource", "https://api.loganalytics.io",
                        "--url", f"https://api.loganalytics.io/v1/workspaces/{ws}/query", "--body", f"@{path}"],
                       capture_output=True, text=True)
finally:
    os.remove(path)
print("\nLog Analytics console lines with a status:")
if r.returncode:
    print("  query failed:", r.stderr[-300:])
else:
    for row in json.loads(r.stdout)["tables"][0]["rows"]:
        print("  ", row[0], row[1], row[2].strip()[:90])
