"""Run a KQL file against appi-skillagent-ka2026 (query sent in a file: no shell quoting issues).
usage: python deploy/azure/appi_query.py deploy/azure/queries/<name>.kql"""
import json
import os
import subprocess
import sys
import tempfile

AZ = "az.cmd" if sys.platform == "win32" else "az"
q = open(sys.argv[1], encoding="utf-8").read()
app_id = subprocess.run([AZ, "monitor", "app-insights", "component", "show", "-g", "DB", "-a", "appi-skillagent-ka2026",
                         "--query", "appId", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()
fd, path = tempfile.mkstemp(suffix=".json")
with os.fdopen(fd, "w") as f:
    json.dump({"query": q}, f)
try:
    r = subprocess.run([AZ, "rest", "--method", "post", "--resource", "https://api.applicationinsights.io", "--url",
                        f"https://api.applicationinsights.io/v1/apps/{app_id}/query", "--body", f"@{path}"],
                       capture_output=True, text=True)
finally:
    os.remove(path)
if r.returncode:
    sys.exit(r.stderr[-400:])
t = json.loads(r.stdout)["tables"][0]
print(" | ".join(c["name"] for c in t["columns"]))
for row in t["rows"]:
    print(" | ".join(str(v) for v in row))
print(f"({len(t['rows'])} rows)")
