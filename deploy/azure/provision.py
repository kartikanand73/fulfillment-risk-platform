"""Phase 6: Azure resources for the scheduled refresh. Rerun-safe; run from the repo root with `az login` as an
Owner of resource group DB. Prints names and ids only - never secrets.

Target (explicit): subscription fe74279e-6ca8-48ff-bf54-90305b257322, resource group DB, region eastus.

  1 identity   user-assigned managed identity id-fr-refresh-ka2026 (the job runs as this)
  2 image      az acr build -> skillagentacrka2026.azurecr.io/fulfillment-risk:<tag>; AcrPull for the identity
  3 keyvault   Key Vault Secrets User on exactly two secrets: salesforce-client-secret, snowflake-fr-pat
  4 postgres   Flexible Server psql-fr-ka2026 (Burstable B1ms, 32 GB, PG 16, Entra-only auth, no passwords),
               database fr, Entra admin = you, firewall: Azure services + this machine's IP
  5 schema     fr schema/views/roles; identity -> PostgreSQL principal with fr_writer
  6 databricks identity -> Databricks service principal: SQL access, CAN_USE warehouse, SELECT on the view only
  7 env        Container Apps environment cae-fr-ka2026 (consumption, logs -> log-skillagent-ka2026)
  8 job        Container Apps Job job-fr-refresh: cron hourly at :07, 0.25 vCPU / 0.5 GiB, 15 min timeout,
               no retries (the next hour retries), App Insights connection string as a secret
  9 run        start one execution now and wait for it

usage: python deploy/azure/provision.py [--tag v1] [--only 4,5] [--skip-build]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from fulfillment_risk.cli import load_dotenv  # noqa: E402

load_dotenv()
SUB, RG, LOC = "fe74279e-6ca8-48ff-bf54-90305b257322", "DB", "eastus"
UAMI, ACR, KV = "id-fr-refresh-ka2026", "skillagentacrka2026", "kv-skillagent-ka2026"
PG, DB = "psql-fr-ka2026", "fr"
CAE, JOB, LAW, APPI = "cae-fr-ka2026", "job-fr-refresh", "log-skillagent-ka2026", "appi-skillagent-ka2026"
CRON = "7 * * * *"
ARM = f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers"
AZ = "az.cmd" if sys.platform == "win32" else "az"
PG_HOST = f"{PG}.postgres.database.azure.com"
SECRETS = ("salesforce-client-secret", "snowflake-fr-pat")


def step(msg):
    print(f"\n[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def az(*a, check=True, out_json=True):
    sub = [] if a[0] == "ad" else ["--subscription", SUB]      # `az ad` is tenant-scoped
    r = subprocess.run([AZ, *a, *sub] + (["-o", "json"] if out_json else []),
                       capture_output=True, text=True)
    if check and r.returncode:
        raise SystemExit(f"az {' '.join(a[:3])} failed: {r.stderr.strip()[-700:]}")
    if not out_json:
        return r.stdout.strip()
    try:
        return json.loads(r.stdout) if r.stdout.strip() else None
    except json.JSONDecodeError:
        return r.stdout.strip()


def arm(method, path, ver, body=None):
    args = ["rest", "--method", method, "--url", f"{ARM}/{path}?api-version={ver}"]
    f = None
    if body is not None:
        fd, f = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as fh:
            json.dump(body, fh)
        args += ["--body", f"@{f}"]
    try:
        r = subprocess.run([AZ, *args], capture_output=True, text=True)
    finally:
        if f:
            os.remove(f)
    if r.returncode:
        if method == "get" and ("ResourceNotFound" in r.stderr or "NotFound" in r.stderr):
            return None
        raise SystemExit(f"ARM {method} {path} failed: {r.stderr.strip()[-700:]}")
    return json.loads(r.stdout) if r.stdout.strip() else {}


def wait_arm(path, ver, key=("properties", "provisioningState"), ok=("Succeeded",), timeout=1800):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = arm("get", path, ver) or {}
        v = r
        for k in key:
            v = (v or {}).get(k)
        if v in ok:
            return r
        if v in ("Failed", "Canceled"):
            raise SystemExit(f"{path}: {v}")
        print(f"   {time.strftime('%H:%M:%S')} {path.split('/')[-1]}: {v}", flush=True)
        time.sleep(20)
    raise SystemExit(f"{path}: timed out")


def pg_connect(dbname, user, token):
    import psycopg
    return psycopg.connect(host=PG_HOST, dbname=dbname, user=user, password=token, sslmode="require", autocommit=True)


# ---------------------------------------------------------------------------------------------------------------
def s1_identity():
    step(f"1 identity {UAMI}")
    mi = az("identity", "create", "-g", RG, "-n", UAMI, "-l", LOC)
    print(f"   clientId {mi['clientId']}  principalId {mi['principalId']}")
    return mi


def s2_image(mi, tag, skip_build):
    step(f"2 image {ACR}.azurecr.io/fulfillment-risk:{tag}")
    if not skip_build:
        az("acr", "build", "-r", ACR, "-t", f"fulfillment-risk:{tag}", ".", "--no-logs", out_json=False)
    acr_id = az("acr", "show", "-n", ACR, "-g", RG, "--query", "id")
    _role(mi["principalId"], "AcrPull", acr_id)


def _role(principal, role, scope):
    have = az("role", "assignment", "list", "--assignee", principal, "--scope", scope, "--role", role,
              "--query", "length(@)")
    if not have:
        for i in range(6):   # a new identity can take a minute to replicate
            r = subprocess.run([AZ, "role", "assignment", "create", "--assignee-object-id", principal,
                                "--assignee-principal-type", "ServicePrincipal", "--role", role, "--scope", scope,
                                "--subscription", SUB, "-o", "none"], capture_output=True, text=True)
            if r.returncode == 0:
                break
            time.sleep(15)
        else:
            raise SystemExit(f"role {role} on {scope}: {r.stderr[-400:]}")
    print(f"   {role} on ...{scope[-60:]}")


def s3_keyvault(mi):
    step("3 Key Vault: Secrets User on the two secrets the job needs (not the whole vault)")
    kv_id = az("keyvault", "show", "-n", KV, "-g", RG, "--query", "id")
    for s in SECRETS:
        _role(mi["principalId"], "Key Vault Secrets User", f"{kv_id}/secrets/{s}")


def s4_postgres():
    step(f"4 PostgreSQL Flexible Server {PG} (Burstable B1ms, 32 GB, PG 16, Entra-only)")
    me = az("ad", "signed-in-user", "show", "--query", "{id:id, upn:userPrincipalName, mail:mail}")
    path = f"Microsoft.DBforPostgreSQL/flexibleServers/{PG}"
    if not arm("get", path, "2024-08-01"):
        arm("put", path, "2024-08-01", {
            "location": LOC, "sku": {"name": "Standard_B1ms", "tier": "Burstable"},
            "properties": {"version": "16", "storage": {"storageSizeGB": 32, "autoGrow": "Disabled"},
                           "authConfig": {"activeDirectoryAuth": "Enabled", "passwordAuth": "Disabled",
                                          "tenantId": az("account", "show", "--query", "tenantId")},
                           "backup": {"backupRetentionDays": 7, "geoRedundantBackup": "Disabled"},
                           "highAvailability": {"mode": "Disabled"}, "network": {"publicNetworkAccess": "Enabled"}}})
    wait_arm(path, "2024-08-01", key=("properties", "state"), ok=("Ready",))
    admin_name = me["mail"] or me["upn"]
    arm("put", f"{path}/administrators/{me['id']}", "2024-08-01",
        {"properties": {"principalType": "User", "principalName": admin_name,
                        "tenantId": az("account", "show", "--query", "tenantId")}})
    wait_arm(f"{path}/administrators/{me['id']}", "2024-08-01")
    print(f"   Entra admin: {admin_name}")
    arm("put", f"{path}/databases/{DB}", "2024-08-01", {"properties": {"charset": "UTF8", "collation": "en_US.utf8"}})
    arm("put", f"{path}/firewallRules/AllowAzureServices", "2024-08-01",
        {"properties": {"startIpAddress": "0.0.0.0", "endIpAddress": "0.0.0.0"}})
    import httpx
    ip = httpx.get("https://api.ipify.org", timeout=20).text.strip()
    arm("put", f"{path}/firewallRules/admin-workstation", "2024-08-01",
        {"properties": {"startIpAddress": ip, "endIpAddress": ip}})
    for rule in ("AllowAzureServices", "admin-workstation"):
        wait_arm(f"{path}/firewallRules/{rule}", "2024-08-01")
    print(f"   database {DB}; firewall: Azure services + {ip}")
    return admin_name


def s5_schema(mi, admin_name):
    step("5 schema + job principal (fr_writer)")
    tok = az("account", "get-access-token", "--resource-type", "oss-rdbms", "--query", "accessToken")
    from fulfillment_risk.store.postgres import SCHEMA_SQL
    with pg_connect(DB, admin_name, tok) as c:
        c.execute(SCHEMA_SQL.read_text())
    with pg_connect("postgres", admin_name, tok) as c:
        exists = c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (UAMI,)).fetchone()
        if not exists:
            c.execute("SELECT * FROM pgaadauth_create_principal_with_oid(%s, %s, 'service', false, false)",
                      (UAMI, mi["principalId"]))
    with pg_connect(DB, admin_name, tok) as c:
        c.execute(f'GRANT fr_writer TO "{UAMI}"')
        c.execute(f"GRANT CONNECT ON DATABASE {DB} TO fr_writer, fr_reader")
        print("   roles:", c.execute("SELECT r.rolname, array_agg(m.rolname) FROM pg_roles r JOIN pg_auth_members a "
                                     "ON a.member=r.oid JOIN pg_roles m ON m.oid=a.roleid WHERE r.rolname=%s GROUP BY 1",
                                     (UAMI,)).fetchall())


def s6_databricks(mi):
    step("6 Databricks: job identity as a read-only service principal")
    import httpx
    host, wh = os.environ["DATABRICKS_HOST"].rstrip("/"), os.environ["DATABRICKS_WAREHOUSE_ID"]
    view = os.environ.get("FR_DATABRICKS_VIEW", "skillagent.sales.v_open_order_lines")
    tok = az("account", "get-access-token", "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d", "--query",
             "accessToken")
    h = {"Authorization": f"Bearer {tok}"}
    c = httpx.Client(base_url=host, headers=h, timeout=120)
    app = mi["clientId"]
    found = c.get("/api/2.0/preview/scim/v2/ServicePrincipals",
                  params={"filter": f'applicationId eq "{app}"'}).json().get("Resources")
    if not found:
        r = c.post("/api/2.0/preview/scim/v2/ServicePrincipals", json={
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"], "applicationId": app,
            "displayName": UAMI, "entitlements": [{"value": "databricks-sql-access"}]})
        r.raise_for_status()
    r = c.patch(f"/api/2.0/permissions/warehouses/{wh}",
                json={"access_control_list": [{"service_principal_name": app, "permission_level": "CAN_USE"}]})
    r.raise_for_status()
    catalog, schema, _ = view.split(".")
    for g in (f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app}`",
              f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{app}`",
              f"GRANT SELECT ON VIEW {view} TO `{app}`"):
        r = c.post("/api/2.0/sql/statements", json={"warehouse_id": wh, "statement": g, "wait_timeout": "50s"}).json()
        while r["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(3)
            r = c.get(f"/api/2.0/sql/statements/{r['statement_id']}").json()
        if r["status"]["state"] != "SUCCEEDED":
            raise SystemExit(f"{g}: {r['status']}")
        print("  ", g)


def s7_env():
    step(f"7 Container Apps environment {CAE}")
    path = f"Microsoft.App/managedEnvironments/{CAE}"
    if not arm("get", path, "2024-03-01"):
        law = az("monitor", "log-analytics", "workspace", "show", "-g", RG, "-n", LAW, "--query", "customerId")
        key = az("monitor", "log-analytics", "workspace", "get-shared-keys", "-g", RG, "-n", LAW,
                 "--query", "primarySharedKey")
        arm("put", path, "2024-03-01", {"location": LOC, "properties": {
            "appLogsConfiguration": {"destination": "log-analytics",
                                     "logAnalyticsConfiguration": {"customerId": law, "sharedKey": key}},
            "workloadProfiles": [{"name": "Consumption", "workloadProfileType": "Consumption"}]}})
    return wait_arm(path, "2024-03-01")["id"]


def s8_job(mi, env_id, tag):
    step(f"8 job {JOB} (cron '{CRON}' UTC)")
    appi = az("monitor", "app-insights", "component", "show", "-g", RG, "-a", APPI, "--query", "connectionString",
              check=False)
    e = os.environ.get
    env = {
        "AZURE_CLIENT_ID": mi["clientId"],
        "FR_DATABASE_ENTRA": "true",
        "FR_DATABASE_URL": f"host={PG_HOST} dbname={DB} user={UAMI} sslmode=require",
        "FR_KEYVAULT_URL": f"https://{KV}.vault.azure.net/",
        "SALESFORCE_MY_DOMAIN": e("SALESFORCE_MY_DOMAIN"), "SALESFORCE_CLIENT_ID": e("SALESFORCE_CLIENT_ID"),
        "SALESFORCE_SECRET_NAME": e("SALESFORCE_SECRET_NAME", "salesforce-client-secret"),
        "DATABRICKS_HOST": e("DATABRICKS_HOST"), "DATABRICKS_WAREHOUSE_ID": e("DATABRICKS_WAREHOUSE_ID"),
        "FR_DATABRICKS_VIEW": e("FR_DATABRICKS_VIEW", "skillagent.sales.v_open_order_lines"),
        "SNOWFLAKE_ACCOUNT_URL": e("SNOWFLAKE_ACCOUNT_URL"),
        "FR_SNOWFLAKE_TABLE": e("FR_SNOWFLAKE_TABLE"), "FR_SNOWFLAKE_WAREHOUSE": e("FR_SNOWFLAKE_WAREHOUSE"),
        "FR_SNOWFLAKE_ROLE": e("FR_SNOWFLAKE_ROLE"), "FR_SNOWFLAKE_SECRET_NAME": e("FR_SNOWFLAKE_SECRET_NAME"),
    }
    missing = [k for k, v in env.items() if not v]
    if missing:
        raise SystemExit(f"missing settings in .env: {missing}")
    env_list = [{"name": k, "value": v} for k, v in env.items()]
    secrets = []
    if appi:
        secrets.append({"name": "appi-conn", "value": appi})
        env_list.append({"name": "APPLICATIONINSIGHTS_CONNECTION_STRING", "secretRef": "appi-conn"})
    arm("put", f"Microsoft.App/jobs/{JOB}", "2024-03-01", {
        "location": LOC,
        "identity": {"type": "UserAssigned", "userAssignedIdentities": {mi["id"]: {}}},
        "properties": {
            "environmentId": env_id, "workloadProfileName": "Consumption",
            "configuration": {
                "triggerType": "Schedule", "scheduleTriggerConfig": {"cronExpression": CRON, "parallelism": 1,
                                                                     "replicaCompletionCount": 1},
                "replicaTimeout": 900, "replicaRetryLimit": 0,
                "registries": [{"server": f"{ACR}.azurecr.io", "identity": mi["id"]}],
                "secrets": secrets},
            "template": {"containers": [{
                "name": "refresh", "image": f"{ACR}.azurecr.io/fulfillment-risk:{tag}",
                "args": ["refresh", "--live", "--trigger", "schedule"],
                "resources": {"cpu": 0.25, "memory": "0.5Gi"}, "env": env_list}]}}})
    wait_arm(f"Microsoft.App/jobs/{JOB}", "2024-03-01")
    print(f"   image fulfillment-risk:{tag}; App Insights {'on' if appi else 'off'}")


def s9_run():
    step("9 start one execution now")
    r = arm("post", f"Microsoft.App/jobs/{JOB}/start", "2024-03-01", {})
    name = (r or {}).get("name") or (r or {}).get("id", "").split("/")[-1]
    print("   execution", name)
    for _ in range(60):
        time.sleep(15)
        ex = arm("get", f"Microsoft.App/jobs/{JOB}/executions/{name}", "2024-03-01") or {}
        st = (ex.get("properties") or {}).get("status")
        print(f"   {time.strftime('%H:%M:%S')} {st}", flush=True)
        if st in ("Succeeded", "Failed", "Stopped", "Degraded"):
            return st
    return "timeout"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="v1")
    ap.add_argument("--only", default="1,2,3,4,5,6,7,8,9")
    ap.add_argument("--skip-build", action="store_true")
    a = ap.parse_args()
    only = {int(x) for x in a.only.split(",")}
    print(f"subscription {SUB} | resource group {RG} | region {LOC}")
    mi = s1_identity()
    if 2 in only:
        s2_image(mi, a.tag, a.skip_build)
    if 3 in only:
        s3_keyvault(mi)
    admin = s4_postgres() if 4 in only else None
    if 5 in only:
        if admin is None:
            me = az("ad", "signed-in-user", "show", "--query", "{upn:userPrincipalName, mail:mail}")
            admin = me["mail"] or me["upn"]
        s5_schema(mi, admin)
    if 6 in only:
        s6_databricks(mi)
    env_id = s7_env() if (7 in only or 8 in only) else None
    if 8 in only:
        s8_job(mi, env_id, a.tag)
    if 9 in only:
        print("   result:", s9_run())
    step("PROVISION_DONE")


if __name__ == "__main__":
    main()
