"""Create this repo's .env from the skill-agent repo's .env, copying ONLY non-secret settings.
usage: python deploy/local_env.py <path-to-other/.env>"""
import sys
from pathlib import Path

KEEP = ("SALESFORCE_MY_DOMAIN", "SALESFORCE_CLIENT_ID", "SNOWFLAKE_ACCOUNT_URL", "DATABRICKS_HOST")
src = dict(l.split("=", 1) for l in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
           if "=" in l and not l.lstrip().startswith("#"))
out = [f"{k}={src[k].strip()}" for k in KEEP if k in src]
out += ["FR_KEYVAULT_URL=https://kv-skillagent-ka2026.vault.azure.net/",
        "SALESFORCE_SECRET_NAME=salesforce-client-secret",
        "DATABRICKS_WAREHOUSE_ID=87f818da0e78701c",
        "FR_DATABRICKS_VIEW=skillagent.sales.v_open_order_lines",
        "FR_SNOWFLAKE_TABLE=SUPPLY_CHAIN_DEMO.INVENTORY.INVENTORY_POSITION",
        "FR_SNOWFLAKE_WAREHOUSE=FR_READER_WH", "FR_SNOWFLAKE_ROLE=FR_READER",
        "FR_SNOWFLAKE_SECRET_NAME=snowflake-fr-pat"]
if Path(".env").exists():
    sys.exit(".env already exists - not overwriting")
Path(".env").write_text("\n".join(out) + "\n", encoding="utf-8")
print("wrote .env with keys:", ", ".join(l.split("=")[0] for l in out))
