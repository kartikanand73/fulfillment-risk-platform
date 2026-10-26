"""Operational signals infrastructure: up | down | deploy | smoke. Rerun-safe. Prints names/ids only.

Target (explicit): subscription fe74279e-6ca8-48ff-bf54-90305b257322, resource group DB, region eastus
(PostgreSQL stays where it is: psql-fr-ka2026, centralus).

up
  1 providers   Microsoft.EventHub, Microsoft.Web registered
  2 identity    id-fr-signals-ka2026 (the Function runs as this)
  3 event hubs  namespace evhns-fr-ka2026 Standard / 1 TU, LOCAL AUTH DISABLED (Entra only), TLS 1.2;
                hub fr-signals: 2 partitions, 24 h retention; consumer group fr-signals-processor
  4 roles       Function identity: Azure Event Hubs Data Receiver on the hub only
                you (signed-in user): Azure Event Hubs Data Sender on the hub only (for the synthetic publisher)
  5 storage     stfrsigka2026 (shared key off) + container app-package; identity: blob owner / queue / table
                (Functions host: leases + checkpoints + deployment package)
  6 postgres    signal tables/views + principal id-fr-signals-ka2026 with role fr_signal_writer only
  7 function    Flex Consumption plan + func-fr-signals-ka2026 (Python 3.11), identity-based settings only
  8 deploy      build zip (function_app + fulfillment_risk.signals) -> remote build
  9 alerts      portal-only scheduled query alerts on appi-skillagent-ka2026 (no action group = no email):
                rejected > 5 in 15 min | function failures | no activity 2 h (created disabled)
down [--drop-tables]
  removes alerts, function app, plan, storage, Event Hubs namespace (and its role assignments), identity,
  the PostgreSQL principal; signal tables are kept unless --drop-tables

usage: python deploy/azure/signals.py up | down [--drop-tables] | deploy | smoke
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from provision import (AZ, DB, PG, RG, SUB, arm, az, pg_connect, step, wait_arm, _role)  # noqa: E402

LOC = "eastus"
UAMI, NS, HUB, CG = "id-fr-signals-ka2026", "evhns-fr-ka2026", "fr-signals", "fr-signals-processor"
ST, CONTAINER, PLAN, FUNC = "stfrsigka2026", "app-package", "plan-fr-signals-ka2026", "func-fr-signals-ka2026"
APPI = "appi-skillagent-ka2026"
ALERTS = ("fr-signals-rejected", "fr-signals-function-failures", "fr-signals-no-activity")
EH_API, WEB_API, ST_API, SQR_API = "2024-01-01", "2024-04-01", "2023-05-01", "2023-03-15-preview"
SUB_ID = f"/subscriptions/{SUB}/resourceGroups/{RG}/providers"


def me():
    return az("ad", "signed-in-user", "show", "--query", "{id:id, upn:userPrincipalName, mail:mail}")


def admin_name():
    m = me()
    return m["mail"] or m["upn"]


def s1_providers():
    step("1 providers")
    for ns in ("Microsoft.EventHub", "Microsoft.Web"):
        if az("provider", "show", "-n", ns, "--query", "registrationState") != "Registered":
            az("provider", "register", "-n", ns, out_json=False)
            for _ in range(60):
                if az("provider", "show", "-n", ns, "--query", "registrationState") == "Registered":
                    break
                time.sleep(10)
        print(f"   {ns}: Registered")


def s2_identity():
    step(f"2 identity {UAMI}")
    mi = az("identity", "create", "-g", RG, "-n", UAMI, "-l", LOC)
    print(f"   clientId {mi['clientId']}  principalId {mi['principalId']}")
    return mi


def s3_eventhubs():
    step(f"3 Event Hubs {NS} (Standard, local auth disabled) / {HUB} (2 partitions) / {CG}")
    p = f"Microsoft.EventHub/namespaces/{NS}"
    if not arm("get", p, EH_API):
        arm("put", p, EH_API, {"location": LOC, "sku": {"name": "Standard", "tier": "Standard", "capacity": 1},
                               "properties": {"disableLocalAuth": True, "minimumTlsVersion": "1.2",
                                              "isAutoInflateEnabled": False, "publicNetworkAccess": "Enabled"}})
    wait_arm(p, EH_API, key=("properties", "provisioningState"))
    arm("put", f"{p}/eventhubs/{HUB}", EH_API, {"properties": {
        "partitionCount": 2, "retentionDescription": {"cleanupPolicy": "Delete", "retentionTimeInHours": 24}}})
    arm("put", f"{p}/eventhubs/{HUB}/consumergroups/{CG}", EH_API,
        {"properties": {"userMetadata": "fulfillment-risk signals Function"}})
    ns = arm("get", p, EH_API)
    print(f"   disableLocalAuth={ns['properties'].get('disableLocalAuth')} endpoint={ns['properties']['serviceBusEndpoint']}")
    return f"{SUB_ID}/{p}/eventhubs/{HUB}"


def s4_roles(mi, hub_id):
    step("4 roles on the hub (least privilege)")
    _role(mi["principalId"], "Azure Event Hubs Data Receiver", hub_id)
    m = me()
    have = az("role", "assignment", "list", "--assignee", m["id"], "--scope", hub_id, "--role",
              "Azure Event Hubs Data Sender", "--query", "[].id")
    if not have:
        az("role", "assignment", "create", "--assignee-object-id", m["id"], "--assignee-principal-type", "User",
           "--role", "Azure Event Hubs Data Sender", "--scope", hub_id, out_json=False)
    print(f"   Azure Event Hubs Data Sender -> {m['mail'] or m['upn']} (publisher)")


def s5_storage(mi):
    step(f"5 storage {ST} (shared key off) for the Functions host + deployment package")
    p = f"Microsoft.Storage/storageAccounts/{ST}"
    if not arm("get", p, ST_API):
        arm("put", p, ST_API, {"location": LOC, "kind": "StorageV2", "sku": {"name": "Standard_LRS"},
                               "properties": {"allowSharedKeyAccess": False, "allowBlobPublicAccess": False,
                                              "minimumTlsVersion": "TLS1_2", "supportsHttpsTrafficOnly": True}})
    wait_arm(p, ST_API)
    arm("put", f"{p}/blobServices/default/containers/{CONTAINER}", ST_API, {"properties": {}})
    sid = f"{SUB_ID}/{p}"
    for role in ("Storage Blob Data Owner", "Storage Queue Data Contributor", "Storage Table Data Contributor"):
        _role(mi["principalId"], role, sid)


def s6_postgres(mi):
    step("6 PostgreSQL: signal tables/views + principal with fr_signal_writer")
    from fulfillment_risk.store.postgres import SCHEMA_SQL, SIGNALS_SQL
    tok = az("account", "get-access-token", "--resource-type", "oss-rdbms", "--query", "accessToken")
    adm = admin_name()
    with pg_connect(DB, adm, tok) as c:
        c.execute(SCHEMA_SQL.read_text())
        c.execute(SIGNALS_SQL.read_text())
    with pg_connect("postgres", adm, tok) as c:
        if not c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (UAMI,)).fetchone():
            c.execute("SELECT * FROM pgaadauth_create_principal_with_oid(%s, %s, 'service', false, false)",
                      (UAMI, mi["principalId"]))
    with pg_connect(DB, adm, tok) as c:
        c.execute(f'GRANT fr_signal_writer TO "{UAMI}"')
        c.execute(f"GRANT CONNECT ON DATABASE {DB} TO fr_signal_writer")
    print(f"   {UAMI}: fr_signal_writer (signal tables only)")


def s7_function(mi):
    step(f"7 Flex Consumption plan {PLAN} + function app {FUNC}")
    plan = f"Microsoft.Web/serverfarms/{PLAN}"
    if not arm("get", plan, WEB_API):
        arm("put", plan, WEB_API, {"location": LOC, "kind": "functionapp",
                                   "sku": {"name": "FC1", "tier": "FlexConsumption"}, "properties": {"reserved": True}})
    appi = az("monitor", "app-insights", "component", "show", "-g", RG, "-a", APPI, "--query", "connectionString")
    settings = {
        "AzureWebJobsStorage__accountName": ST,
        "AzureWebJobsStorage__credential": "managedidentity",
        "AzureWebJobsStorage__clientId": mi["clientId"],
        "EventHubConnection__fullyQualifiedNamespace": f"{NS}.servicebus.windows.net",
        "EventHubConnection__credential": "managedidentity",
        "EventHubConnection__clientId": mi["clientId"],
        "EVENTHUB_NAME": HUB, "EVENTHUB_CONSUMER_GROUP": CG,
        "AZURE_CLIENT_ID": mi["clientId"],
        "FR_DATABASE_URL": f"host={PG}.postgres.database.azure.com dbname={DB} user={UAMI}",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": appi,
    }
    arm("put", f"Microsoft.Web/sites/{FUNC}", WEB_API, {
        "location": LOC, "kind": "functionapp,linux",
        "identity": {"type": "UserAssigned", "userAssignedIdentities": {mi["id"]: {}}},
        "properties": {
            "serverFarmId": f"{SUB_ID}/{plan}", "httpsOnly": True,
            "functionAppConfig": {
                "deployment": {"storage": {"type": "blobContainer",
                                           "value": f"https://{ST}.blob.core.windows.net/{CONTAINER}",
                                           "authentication": {"type": "UserAssignedIdentity",
                                                              "userAssignedIdentityResourceId": mi["id"]}}},
                "scaleAndConcurrency": {"maximumInstanceCount": 40, "instanceMemoryMB": 2048},
                "runtime": {"name": "python", "version": "3.11"}},
            "siteConfig": {"minTlsVersion": "1.2", "ftpsState": "Disabled",
                           "appSettings": [{"name": k, "value": v} for k, v in settings.items()]}}})
    wait_arm(f"Microsoft.Web/sites/{FUNC}", WEB_API, key=("properties", "state"), ok=("Running",))
    print(f"   {FUNC}: Running (settings: identity-based, no secrets except the App Insights connection string)")


def s8_deploy():
    step("8 deploy code (remote build)")
    zp = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "build_function.py")],
                        capture_output=True, text=True, check=True).stdout.strip()
    out = az("functionapp", "deployment", "source", "config-zip", "-g", RG, "-n", FUNC, "--src", zp,
             "--build-remote", "true", "--timeout", "900", check=False, out_json=False)
    print("   ", (out or "")[-300:].replace("\n", " "))
    for _ in range(30):
        fns = az("functionapp", "function", "list", "-g", RG, "-n", FUNC, "--query", "[].name", check=False)
        if fns:
            print("   functions:", fns)
            return
        time.sleep(10)
    print("   functions not listed yet (indexing can take a minute)")


def _alert(name, desc, query, agg, column, threshold, op, window, freq, sev, enabled=True):
    appi_id = az("monitor", "app-insights", "component", "show", "-g", RG, "-a", APPI, "--query", "id")
    crit = {"query": query, "timeAggregation": agg, "operator": op, "threshold": threshold,
            "failingPeriods": {"numberOfEvaluationPeriods": 1, "minFailingPeriodsToAlert": 1}}
    if column:
        crit["metricMeasureColumn"] = column
    arm("put", f"Microsoft.Insights/scheduledQueryRules/{name}", SQR_API, {
        "location": LOC, "kind": "LogAlert",
        "properties": {"displayName": name, "description": desc, "severity": sev, "enabled": enabled,
                       "scopes": [appi_id], "evaluationFrequency": freq, "windowSize": window,
                       "criteria": {"allOf": [crit]}, "autoMitigate": True, "actions": {"actionGroups": []}}})
    print(f"   {name}: {'enabled' if enabled else 'created disabled'} (portal only)")


def s9_alerts():
    step("9 alerts (portal only - no action group)")
    base = f"traces | where cloud_RoleName == '{FUNC}' and message startswith 'fr_signals_batch'"
    _alert("fr-signals-rejected", "More than 5 signal events rejected in 15 minutes (bad producer or schema drift)",
           base + " | extend rejected = toint(extract(@'rejected=(\\d+)', 1, message)) | project timestamp, rejected",
           "Total", "rejected", 5, "GreaterThan", "PT15M", "PT5M", 2)
    _alert("fr-signals-function-failures", "The signals Function failed (batch will be retried; check exceptions)",
           f"requests | where cloud_RoleName == '{FUNC}' and success == false",
           "Count", None, 0, "GreaterThan", "PT5M", "PT5M", 1)
    _alert("fr-signals-no-activity", "No signal batches processed for 2 hours (enable when a real producer exists)",
           base, "Count", None, 1, "LessThan", "PT2H", "PT30M", 3, enabled=False)


def smoke():
    step("smoke: publish the 'normal' scenario and read it back")
    from fulfillment_risk.signals.publisher import build, publish
    m = build("normal", "smoke" + time.strftime("%H%M%S"))
    print("   sent", publish(m, f"{NS}.servicebus.windows.net", HUB))
    tok = az("account", "get-access-token", "--resource-type", "oss-rdbms", "--query", "accessToken")
    for i in range(24):
        time.sleep(5)
        with pg_connect(DB, admin_name(), tok) as c:
            n = c.execute("SELECT count(*) FROM fr.signal_event WHERE event_id = ANY(%s::uuid[])",
                          (list(m.event_ids()),)).fetchone()[0]
        print(f"   {(i + 1) * 5}s: {n}/{len(m.event_ids())} stored", flush=True)
        if n == len(m.event_ids()):
            return True
    return False


def down(drop_tables: bool):
    step("down: alerts, function app, plan, storage, Event Hubs, identity, PostgreSQL principal")
    for a in ALERTS:
        az("resource", "delete", "--ids", f"{SUB_ID}/Microsoft.Insights/scheduledQueryRules/{a}", check=False,
           out_json=False)
    for rid in (f"Microsoft.Web/sites/{FUNC}", f"Microsoft.Web/serverfarms/{PLAN}",
                f"Microsoft.Storage/storageAccounts/{ST}", f"Microsoft.EventHub/namespaces/{NS}"):
        r = az("resource", "delete", "--ids", f"{SUB_ID}/{rid}", check=False, out_json=False)
        print(f"   deleted {rid.split('/')[-1]}")
    tok = az("account", "get-access-token", "--resource-type", "oss-rdbms", "--query", "accessToken")
    adm = admin_name()
    with pg_connect(DB, adm, tok) as c:
        c.execute(f'REVOKE fr_signal_writer FROM "{UAMI}"') if c.execute(
            "SELECT 1 FROM pg_roles WHERE rolname=%s", (UAMI,)).fetchone() else None
        if drop_tables:
            c.execute("DROP VIEW IF EXISTS fr.v_signal_recent, fr.v_signal_summary, fr.v_signal_rejected_recent; "
                      "DROP TABLE IF EXISTS fr.signal_event, fr.signal_rejected, fr.signal_entity_state")
            print("   dropped signal tables and views")
    with pg_connect("postgres", adm, tok) as c:
        if c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (UAMI,)).fetchone():
            c.execute(f'DROP ROLE "{UAMI}"')
            print(f"   dropped PostgreSQL principal {UAMI}")
    az("identity", "delete", "-g", RG, "-n", UAMI, check=False, out_json=False)
    print(f"   deleted identity {UAMI}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "up"
    print(f"subscription {SUB} | resource group {RG} | region {LOC} | command {cmd}")
    if cmd == "up":
        s1_providers()
        mi = s2_identity()
        hub = s3_eventhubs()
        s4_roles(mi, hub)
        s5_storage(mi)
        s6_postgres(mi)
        s7_function(mi)
        s8_deploy()
        s9_alerts()
    elif cmd == "deploy":
        s8_deploy()
    elif cmd == "alerts":
        s9_alerts()
    elif cmd == "smoke":
        print("   result:", "OK" if smoke() else "NOT ALL STORED")
    elif cmd == "down":
        down("--drop-tables" in sys.argv)
    else:
        sys.exit(__doc__)
    step("SIGNALS_DONE")


if __name__ == "__main__":
    main()
