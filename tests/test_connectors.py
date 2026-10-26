"""Live connectors against mocked HTTP: request shape (read-only), paging, mapping to the contract, failures.
Contract parity: fed the synthetic dataset, each connector must return exactly the synthetic adapter's records."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from fulfillment_risk.engine import compute
from fulfillment_risk.sources import SourceError
from fulfillment_risk.sources.auth import require_select
from fulfillment_risk.sources.databricks import COLUMNS as DBX_COLS
from fulfillment_risk.sources.databricks import DatabricksConfig, DatabricksOpenOrders
from fulfillment_risk.sources.salesforce import SalesforceConfig, SalesforceCustomers, tier_from_employees
from fulfillment_risk.sources.snowflake import SnowflakeConfig, SnowflakeInventory
from fulfillment_risk.sources.synthetic import CUSTOMERS, INVENTORY, OPEN_LINES

EMP = {"Enterprise": 5000, "Mid-Market": 250, "SMB": 10}


# ---------------- Salesforce ----------------
class FakeSF:
    def __init__(self, token_status=200, page_size=2, query_status=200):
        self.reqs, self.token_status, self.page, self.qs = [], token_status, page_size, query_status
        self.recs = [{"Id": c.customer_id, "Name": c.name, "NumberOfEmployees": EMP[c.tier],
                      "Owner": {"Name": c.owner}} for c in CUSTOMERS]

    def __call__(self, req: httpx.Request):
        self.reqs.append(req)
        u = req.url
        if u.path == "/services/oauth2/token":
            assert req.method == "POST" and b"grant_type=client_credentials" in req.content
            return httpx.Response(self.token_status, json={"access_token": "T", "instance_url": "https://inst.example"}
                                  if self.token_status == 200 else {"error": "invalid_client"})
        assert req.method == "GET" and req.headers["Authorization"] == "Bearer T"
        if self.qs != 200:
            return httpx.Response(self.qs, json=[{"message": "INVALID_FIELD: No such column", "errorCode": "x"}])
        start = int(u.params.get("start", 0)) if u.path.endswith("/next") else 0
        if not u.path.endswith("/next"):
            q = u.params["q"]
            assert q.startswith("SELECT ") and " FROM Account" in q
        chunk = self.recs[start:start + self.page]
        nxt = start + self.page
        done = nxt >= len(self.recs)
        body = {"records": chunk, "done": done}
        if not done:
            body["nextRecordsUrl"] = f"/services/data/v62.0/query/next?start={nxt}"
        return httpx.Response(200, json=body)


def sf(fake, now=None, **cfg):
    return SalesforceCustomers(SalesforceConfig("https://my.example", "CID", **cfg),
                               http=httpx.Client(transport=httpx.MockTransport(fake)), secret_provider=lambda: "S",
                               clock=(lambda: now) if now else None)


def test_salesforce_parity_and_paging():
    fake = FakeSF(page_size=2)
    b = sf(fake).fetch_customers()
    assert b.rows == CUSTOMERS and b.source == "salesforce" and b.as_of.tzinfo
    assert sum(1 for r in fake.reqs if "/query" in r.url.path) == 3            # 5 records, 2 per page
    assert all(r.method in ("GET", "POST") for r in fake.reqs)
    assert [r.method for r in fake.reqs if r.url.path != "/services/oauth2/token"] == ["GET"] * 3   # read-only


def test_salesforce_tier_field_and_filter():
    fake = FakeSF()
    for r in fake.recs:
        r["Tier__c"] = "Enterprise"
    b = sf(fake, tier_field="Tier__c", where="Type = 'Customer'").fetch_customers()
    assert {c.tier for c in b.rows} == {"Enterprise"}
    q = [r for r in fake.reqs if r.url.path.endswith("/query")][0].url.params["q"]
    assert "Tier__c" in q and "WHERE Type = 'Customer'" in q


def test_salesforce_failures_are_source_errors_without_secrets():
    with pytest.raises(SourceError, match="token request failed"):
        sf(FakeSF(token_status=400)).fetch_customers()
    with pytest.raises(SourceError, match="INVALID_FIELD") as e:
        sf(FakeSF(query_status=400)).fetch_customers()
    assert "S" != str(e.value) and "Bearer" not in str(e.value)


def test_tier_from_employees():
    assert [tier_from_employees(x) for x in (None, "abc", 99, 100, 999, 1000)] == \
           ["SMB", "SMB", "SMB", "Mid-Market", "Mid-Market", "Enterprise"]


# ---------------- Databricks ----------------
def dbx_rows():
    return [[l.order_id, str(l.line_number), l.customer_id, l.product_sku, str(l.quantity), str(l.unit_net_value),
             l.order_date.isoformat(), l.status, "2026-10-26T12:00:00.000Z"] for l in OPEN_LINES]


class FakeDBX:
    def __init__(self, final="SUCCEEDED", cols=DBX_COLS, chunked=True, http_status=200):
        self.reqs, self.final, self.cols, self.chunked, self.http_status, self.polls = [], final, cols, chunked, http_status, 0

    def __call__(self, req: httpx.Request):
        self.reqs.append(req)
        assert req.headers["Authorization"] == "Bearer DT"
        if self.http_status != 200:
            return httpx.Response(self.http_status, json={"message": "PERMISSION_DENIED"})
        rows = dbx_rows()
        if req.method == "POST":
            body = json.loads(req.content)
            assert body["warehouse_id"] == "WH" and body["statement"].startswith("SELECT ")
            assert "v_open_order_lines" in body["statement"]
            return httpx.Response(200, json={"statement_id": "S1", "status": {"state": "PENDING"}})
        if req.url.path == "/api/2.0/sql/statements/S1":
            self.polls += 1
            if self.final != "SUCCEEDED":
                return httpx.Response(200, json={"statement_id": "S1", "status": {
                    "state": self.final, "error": {"message": "TABLE_OR_VIEW_NOT_FOUND"}}})
            first = rows[:4] if self.chunked else rows
            res = {"data_array": first}
            if self.chunked:
                res["next_chunk_internal_link"] = "/api/2.0/sql/statements/S1/result/chunks/1"
            return httpx.Response(200, json={"statement_id": "S1", "status": {"state": "SUCCEEDED"},
                                             "manifest": {"schema": {"columns": [{"name": c} for c in self.cols]}},
                                             "result": res})
        if req.url.path.endswith("/chunks/1"):
            return httpx.Response(200, json={"data_array": rows[4:]})
        return httpx.Response(404, json={"message": "unexpected"})


def dbx(fake):
    return DatabricksOpenOrders(DatabricksConfig("https://adb.example", "WH"),
                                http=httpx.Client(transport=httpx.MockTransport(fake)),
                                token_provider=lambda: "DT", sleep=lambda s: None)


def test_databricks_parity_poll_and_chunks():
    fake = FakeDBX()
    b = dbx(fake).fetch_open_order_lines()
    assert b.rows == OPEN_LINES and b.as_of.isoformat() == "2026-10-26T12:00:00+00:00"
    assert fake.polls == 1 and [r.method for r in fake.reqs].count("POST") == 1     # the only POST is the SELECT


@pytest.mark.parametrize("fake,match", [(FakeDBX(final="FAILED"), "TABLE_OR_VIEW_NOT_FOUND"),
                                        (FakeDBX(cols=DBX_COLS[:-1]), "unexpected columns"),
                                        (FakeDBX(http_status=403), "403")])
def test_databricks_failures(fake, match):
    with pytest.raises(SourceError, match=match):
        dbx(fake).fetch_open_order_lines()


# ---------------- Snowflake ----------------
class FakeSnow:
    def __init__(self, async_first=True, partitions=2, status=200):
        self.reqs, self.async_first, self.parts, self.status = [], async_first, partitions, status

    def __call__(self, req: httpx.Request):
        self.reqs.append(req)
        assert req.headers["Authorization"] == "Bearer PAT"
        assert req.headers["X-Snowflake-Authorization-Token-Type"] == "PROGRAMMATIC_ACCESS_TOKEN"
        if self.status != 200:
            return httpx.Response(self.status, json={"message": "Insufficient privileges"})
        rows = [[p.product_sku, p.location, str(p.on_hand), str(p.reserved), "2026-10-26T12:00:00.000"]
                for p in INVENTORY]
        meta = {"rowType": [{"name": n} for n in ("PRODUCT_SKU", "LOCATION", "ON_HAND", "RESERVED", "READ_AT")],
                "partitionInfo": [{}] * self.parts}
        if req.method == "POST":
            body = json.loads(req.content)
            assert body["statement"].startswith("SELECT ") and body["role"] == "FR_READER"
            if self.async_first:
                return httpx.Response(202, json={"statementHandle": "H1"})
        part = int(req.url.params.get("partition", 0))
        if part == 1:
            return httpx.Response(200, json={"data": rows[4:]})
        return httpx.Response(200, json={"statementHandle": "H1", "resultSetMetaData": meta,
                                         "data": rows[:4] if self.parts > 1 else rows})


def snow(fake):
    return SnowflakeInventory(SnowflakeConfig("https://acct.example"),
                              http=httpx.Client(transport=httpx.MockTransport(fake)),
                              pat_provider=lambda: "PAT", sleep=lambda s: None)


def test_snowflake_parity_async_and_partitions():
    fake = FakeSnow()
    b = snow(fake).fetch_inventory()
    assert b.rows == INVENTORY and b.as_of.isoformat() == "2026-10-26T12:00:00+00:00"
    assert [r.method for r in fake.reqs] == ["POST", "GET", "GET"]


def test_snowflake_denied():
    with pytest.raises(SourceError, match="403 Insufficient privileges"):
        snow(FakeSnow(status=403)).fetch_inventory()


# ---------------- end to end on mocked live connectors ----------------
def test_live_connectors_give_same_answer_as_synthetic():
    from fulfillment_risk.sources.synthetic import synthetic_sources
    s = synthetic_sources()
    expected = compute(s[0].fetch_customers().rows, s[1].fetch_open_order_lines().rows, s[2].fetch_inventory().rows)
    got = compute(sf(FakeSF()).fetch_customers().rows, dbx(FakeDBX()).fetch_open_order_lines().rows,
                  snow(FakeSnow()).fetch_inventory().rows)
    assert got == expected


def test_live_dry_run_gates_apply():
    from fulfillment_risk.refresh import RefreshSettings, dry_run
    noon = datetime(2026, 10, 26, 12, 30, tzinfo=timezone.utc)   # mocked Databricks/Snowflake read at 12:00
    out = dry_run(sf(FakeSF(), now=noon), dbx(FakeDBX()), snow(FakeSnow()), now=noon)
    assert out.status == "succeeded" and out.result.summary["customers_at_risk"] == 2
    late = dry_run(sf(FakeSF(), now=noon), dbx(FakeDBX()), snow(FakeSnow()),
                   settings=RefreshSettings(max_age_hours=0.25), now=noon)
    assert late.status == "failed" and "stale" in late.error
    down = dry_run(sf(FakeSF()), dbx(FakeDBX(http_status=500)), snow(FakeSnow()))
    assert down.status == "failed" and "databricks" in down.error


# ---------------- read-only guard ----------------
@pytest.mark.parametrize("sql", ["DELETE FROM t", "UPDATE t SET x=1", "SELECT 1; DROP TABLE t", "MERGE INTO t",
                                 "CREATE TABLE t (x int)", ""])
def test_require_select_blocks_writes(sql):
    with pytest.raises(ValueError):
        require_select(sql)


def test_require_select_allows_reads():
    assert require_select(" select 1 ;") == "select 1" and require_select("WITH a AS (SELECT 1) SELECT * FROM a")


def test_live_adapters_are_read_only_objects():
    for a in (sf(FakeSF()), dbx(FakeDBX()), snow(FakeSnow())):
        public = [n for n in dir(a) if not n.startswith("_") and callable(getattr(a, n))]
        assert all(n.startswith("fetch_") for n in public), public


_ = timedelta
