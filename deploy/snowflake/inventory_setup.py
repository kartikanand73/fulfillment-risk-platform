"""Phase 5 / Snowflake: synthetic inventory + a least-privilege service identity for the refresh.

Runs as the existing admin PAT (Key Vault `snowflake-pat`). Creates, rerun-safe:
  warehouse  FR_READER_WH  (XSMALL, auto-suspend 60 s, starts suspended)
  table      SUPPLY_CHAIN_DEMO.INVENTORY.INVENTORY_POSITION (PRODUCT_SKU, LOCATION, ON_HAND, RESERVED, UPDATED_AT)
             - stock for the 20 Databricks SKUs, deliberately short on P-1001, P-1003 and P-4005
  role       FR_READER: USAGE on warehouse/database/schema + SELECT on that one table, nothing else
  user       FR_REFRESH_SVC (TYPE=SERVICE) with role FR_READER, PAT-only authentication policy
  PAT        FR_REFRESH (role-restricted to FR_READER, 90 days) -> Key Vault `snowflake-fr-pat`
             (written via a temp file; never printed). Pass --rotate to issue a new one.
usage: python deploy/snowflake/inventory_setup.py [--rotate]
"""
import json
import os
import subprocess
import sys
import tempfile
import time

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from fulfillment_risk.cli import load_dotenv  # noqa: E402

load_dotenv()
URL = os.environ["SNOWFLAKE_ACCOUNT_URL"].rstrip("/")
VAULT = os.environ.get("FR_KEYVAULT_URL", "https://kv-skillagent-ka2026.vault.azure.net/").split("//")[1].split(".")[0]
AZ = "az.cmd" if sys.platform == "win32" else "az"
DB, SCHEMA, TABLE, WH, ROLE, USER = "SUPPLY_CHAIN_DEMO", "INVENTORY", "INVENTORY_POSITION", "FR_READER_WH", "FR_READER", "FR_REFRESH_SVC"
FQ = f"{DB}.{SCHEMA}.{TABLE}"


def kv(name):
    r = subprocess.run([AZ, "keyvault", "secret", "show", "--vault-name", VAULT, "-n", name, "--query", "value",
                        "-o", "tsv"], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


ADMIN_PAT = kv("snowflake-pat")
if not ADMIN_PAT:
    sys.exit("could not read snowflake-pat from Key Vault")
H = {"Authorization": f"Bearer {ADMIN_PAT}", "Content-Type": "application/json", "Accept": "application/json",
     "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN"}
http = httpx.Client(timeout=120)


def sql(stmt, warehouse=None, quiet=False):
    body = {"statement": stmt, "timeout": 120}
    if warehouse:
        body["warehouse"] = warehouse
    r = http.post(f"{URL}/api/v2/statements", headers=H, json=body)
    while r.status_code == 202:
        time.sleep(2)
        r = http.get(f"{URL}/api/v2/statements/{r.json()['statementHandle']}", headers=H)
    if r.status_code != 200:
        msg = r.json().get("message", r.text)[:300] if r.headers.get("content-type", "").startswith("application/json") else r.text[:300]
        raise SystemExit(f"FAILED ({r.status_code}): {msg}\n  :: {stmt[:160]}")
    j = r.json()
    if not quiet:
        print("  ok:", " ".join(stmt.split())[:100])
    cols = [c["name"].lower() for c in j["resultSetMetaData"]["rowType"]]
    return [dict(zip(cols, row)) for row in j.get("data") or []]


print("connected as", sql("SELECT CURRENT_USER() AS u, CURRENT_ROLE() AS r", quiet=True))

# demand in Databricks (Processing orders) per SKU, for reference:
#   P-1001 56 | P-1003 23 | P-1004 19 | P-1005 35 | P-4001 26 | P-4002 55 | P-4003 20 | P-4004 33 | P-4005 61 | P-4006 39
STOCK = [  # sku, location, on_hand, reserved
    ("P-1001", "EAST-DC", 40, 0),                                   # 40 < 56 -> the later order (Initech) is short 16
    ("P-1002", "EAST-DC", 60, 0),
    ("P-1003", "WEST-DC", 10, 10),                                  # all reserved -> Globex's 23 tablets unfulfillable
    ("P-1004", "EAST-DC", 30, 0), ("P-1005", "WEST-DC", 50, 5),
    ("P-2001", "DIGITAL", 1000, 0), ("P-2002", "DIGITAL", 1000, 0), ("P-2003", "DIGITAL", 1000, 0),
    ("P-2004", "DIGITAL", 1000, 0), ("P-2005", "DIGITAL", 1000, 0),
    ("P-3001", "SERVICES", 100, 0), ("P-3002", "SERVICES", 100, 0), ("P-3003", "SERVICES", 100, 0),
    ("P-3004", "SERVICES", 100, 0),
    ("P-4001", "EAST-DC", 80, 0), ("P-4002", "EAST-DC", 70, 0), ("P-4003", "WEST-DC", 25, 0),
    ("P-4004", "EAST-DC", 50, 10),
    ("P-4005", "EAST-DC", 30, 0), ("P-4005", "WEST-DC", 20, 5),     # 45 < 61 -> Globex (latest) gets 4 of 20
    ("P-4006", "EAST-DC", 60, 0),
]

sql(f"CREATE WAREHOUSE IF NOT EXISTS {WH} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE "
    "INITIALLY_SUSPENDED = TRUE COMMENT = 'fulfillment-risk refresh (read-only)'")
sql(f"CREATE DATABASE IF NOT EXISTS {DB} COMMENT = 'Synthetic supply-chain data for the fulfillment-risk demo'")
sql(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")
sql(f"""CREATE OR REPLACE TABLE {FQ} (
  PRODUCT_SKU VARCHAR NOT NULL COMMENT 'Same SKU as Databricks order_lines.product_id',
  LOCATION    VARCHAR NOT NULL,
  ON_HAND     NUMBER(10,0) NOT NULL,
  RESERVED    NUMBER(10,0) NOT NULL DEFAULT 0,
  UPDATED_AT  TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (PRODUCT_SKU, LOCATION)
) COMMENT = 'Current inventory by SKU and location (synthetic)'""")
values = ", ".join(f"('{s}', '{l}', {o}, {r})" for s, l, o, r in STOCK)
sql(f"INSERT INTO {FQ} (PRODUCT_SKU, LOCATION, ON_HAND, RESERVED) VALUES {values}", warehouse=WH)

sql(f"CREATE ROLE IF NOT EXISTS {ROLE} COMMENT = 'fulfillment-risk refresh: read inventory only'")
for g in (f"GRANT USAGE ON WAREHOUSE {WH} TO ROLE {ROLE}", f"GRANT USAGE ON DATABASE {DB} TO ROLE {ROLE}",
          f"GRANT USAGE ON SCHEMA {DB}.{SCHEMA} TO ROLE {ROLE}", f"GRANT SELECT ON TABLE {FQ} TO ROLE {ROLE}"):
    sql(g)
sql(f"CREATE USER IF NOT EXISTS {USER} TYPE = SERVICE DEFAULT_ROLE = {ROLE} DEFAULT_WAREHOUSE = {WH} "
    "COMMENT = 'fulfillment-risk refresh job (read-only)'")
sql(f"GRANT ROLE {ROLE} TO USER {USER}")
sql(f"CREATE AUTHENTICATION POLICY IF NOT EXISTS {DB}.{SCHEMA}.FR_PAT_ONLY "
    "AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN') "
    "PAT_POLICY = (NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED) "
    "COMMENT = 'Service user may only use PATs; the Azure job has no fixed egress IP'")
try:
    sql(f"ALTER USER {USER} SET AUTHENTICATION POLICY {DB}.{SCHEMA}.FR_PAT_ONLY")
except SystemExit as e:
    if "already" not in str(e).lower():
        raise
    print("  (authentication policy already set)")

if kv("snowflake-fr-pat") and "--rotate" not in sys.argv:
    print("Key Vault already has snowflake-fr-pat; not issuing a new PAT (use --rotate)")
else:
    try:   # rotate / first run after a partial setup: drop an existing token of the same name
        sql(f"ALTER USER {USER} REMOVE PROGRAMMATIC ACCESS TOKEN FR_REFRESH", quiet=True)
    except SystemExit:
        pass
    rows = sql(f"ALTER USER {USER} ADD PROGRAMMATIC ACCESS TOKEN FR_REFRESH ROLE_RESTRICTION = '{ROLE}' "
               "DAYS_TO_EXPIRY = 90 COMMENT = 'fulfillment-risk refresh'", quiet=True)
    secret = rows[0].get("token_secret") if rows else None
    if not secret:
        sys.exit("PAT created but no token_secret returned")
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as f:
            f.write(secret)
        r = subprocess.run([AZ, "keyvault", "secret", "set", "--vault-name", VAULT, "-n", "snowflake-fr-pat",
                            "--file", path, "--expires", time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                                       time.gmtime(time.time() + 89 * 86400)),
                            "-o", "none"], capture_output=True, text=True)
        if r.returncode:
            sys.exit("could not store the PAT in Key Vault: " + r.stderr[-300:])
    finally:
        os.remove(path)
    print("PAT FR_REFRESH stored in Key Vault as snowflake-fr-pat (expires in 90 days)")

print(sql(f"SELECT COUNT(*) AS rows, SUM(ON_HAND - RESERVED) AS available FROM {FQ}", warehouse=WH, quiet=True))
print("SNOWFLAKE_DONE")
