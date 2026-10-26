"""Recent executions of the refresh job + the published snapshot status."""
import json
import subprocess
import sys

AZ = "az.cmd" if sys.platform == "win32" else "az"
r = subprocess.run([AZ, "containerapp", "job", "execution", "list", "-g", "DB", "-n", "job-fr-refresh", "-o", "json"],
                   capture_output=True, text=True, check=True)
for e in sorted(json.loads(r.stdout), key=lambda e: e["properties"]["startTime"], reverse=True)[:10]:
    p = e["properties"]
    print(f"{p['startTime']}  {p['status']:10}  {e['name']}")
