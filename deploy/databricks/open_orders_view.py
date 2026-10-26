"""Phase 5 / Databricks: the read-only view the refresh queries with Databricks SQL (never Genie).

  skillagent.sales.v_open_order_lines = order lines of orders not yet shipped (status = 'Processing')
usage: python deploy/databricks/open_orders_view.py        (az login as a workspace admin; rerun-safe)
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from fulfillment_risk.cli import load_dotenv  # noqa: E402

load_dotenv()
HOST, WH = os.environ["DATABRICKS_HOST"].rstrip("/"), os.environ["DATABRICKS_WAREHOUSE_ID"]
VIEW = os.environ.get("FR_DATABRICKS_VIEW", "skillagent.sales.v_open_order_lines")
AZ = "az.cmd" if sys.platform == "win32" else "az"
TOKEN = subprocess.run([AZ, "account", "get-access-token", "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                        "--query", "accessToken", "-o", "tsv"], capture_output=True, text=True, check=True).stdout.strip()


def call(method, path, body=None):
    req = urllib.request.Request(HOST + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read() or b"{}")


def sql(stmt):
    r = call("POST", "/api/2.0/sql/statements", {"warehouse_id": WH, "statement": stmt, "wait_timeout": "50s",
                                                  "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = call("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        sys.exit(f"FAILED: {r['status'].get('error')} :: {stmt[:120]}")
    return (r.get("result") or {}).get("data_array") or []


schema = VIEW.rsplit(".", 1)[0]
sql(f"""CREATE OR REPLACE VIEW {VIEW}
COMMENT 'Open (not yet shipped) order lines for the fulfillment-risk refresh. Read-only contract: see fulfillment-risk-platform docs/data-contract.md'
AS SELECT o.order_id,
          l.line_number,
          o.salesforce_account_id                          AS customer_id,
          l.product_id                                     AS product_sku,
          l.quantity,
          CAST(l.net_amount / l.quantity AS DECIMAL(14,4)) AS unit_net_value,
          o.order_date,
          o.status
   FROM {schema}.orders o JOIN {schema}.order_lines l ON l.order_id = o.order_id
   WHERE o.status = 'Processing'""")
print("view", VIEW, "ready")
for r in sql(f"SELECT customer_id, count(DISTINCT order_id), count(*), sum(quantity) FROM {VIEW} GROUP BY 1 ORDER BY 1"):
    print("  ", r)
