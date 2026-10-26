"""Show recent refresh spans from Application Insights (query sent in a file: no shell quoting issues).
usage: python deploy/azure/check_traces.py [minutes]"""
import json
import os
import subprocess
import sys
import tempfile

AZ = "az.cmd" if sys.platform == "win32" else "az"
mins = int(sys.argv[1]) if len(sys.argv) > 1 else 60
roots_only = "--roots" in sys.argv
app_id = subprocess.run([AZ, "monitor", "app-insights", "component", "show", "-g", "DB", "-a", "appi-skillagent-ka2026",
                         "--query", "appId", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()
q = f"""union requests, dependencies
| where timestamp > ago({mins}m) and cloud_RoleName == 'fulfillment-risk-refresh'
{"| where name == 'fulfillment_risk.refresh'" if roots_only else ""}
| project timestamp, itemType, name, success, duration,
          status=tostring(customDimensions['fr.status']), run_id=tostring(customDimensions['fr.run_id']),
          at_risk=tostring(customDimensions['fr.customers_at_risk']), short=tostring(customDimensions['fr.short_value'])
| order by timestamp desc | take 20"""
fd, path = tempfile.mkstemp(suffix=".json")
with os.fdopen(fd, "w") as f:
    json.dump({"query": q}, f)
try:
    r = subprocess.run([AZ, "rest", "--method", "post", "--resource", "https://api.applicationinsights.io",
                        "--url", f"https://api.applicationinsights.io/v1/apps/{app_id}/query", "--body", f"@{path}"],
                       capture_output=True, text=True)
finally:
    os.remove(path)
if r.returncode:
    sys.exit(r.stderr[-500:])
t = json.loads(r.stdout)["tables"][0]
cols = [c["name"] for c in t["columns"]]
print(" | ".join(cols))
for row in t["rows"]:
    print(" | ".join(str(v) for v in row))
print(f"({len(t['rows'])} rows)")
