"""Run one traced dry run locally (App Insights connection string read via az, never printed)."""
import os
import subprocess
import sys

AZ = "az.cmd" if sys.platform == "win32" else "az"
os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = subprocess.run(
    [AZ, "monitor", "app-insights", "component", "show", "-g", "DB", "-a", "appi-skillagent-ka2026", "--query",
     "connectionString", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from fulfillment_risk.cli import main  # noqa: E402

rc = main(["refresh", "--live", "--dry-run", "--trigger", "trace-test"])
from opentelemetry import trace  # noqa: E402
p = trace.get_tracer_provider()
print("provider:", type(p).__name__, "rc:", rc)
