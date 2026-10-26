"""Refresh + PostgreSQL snapshot: publish, failure handling, concurrency, freshness, access control."""
from datetime import timedelta
from decimal import Decimal

import psycopg
import pytest

from fulfillment_risk.refresh import RefreshSettings, run_refresh
from fulfillment_risk.sources.synthetic import (ACME, EXPECTED, INITECH, Scenario, line, synthetic_sources)
from fulfillment_risk.store import SnapshotStore, read_customers, read_lines, read_status
from fulfillment_risk.store.postgres import LOCK_KEY

NOW = Scenario().now


def refresh(dsn, sc=None, **kw):
    sc = sc or Scenario()
    return run_refresh(dsn, *synthetic_sources(sc), now=sc.now, trigger="test", **kw)


def runs(dsn):
    with psycopg.connect(dsn) as c:
        return c.execute("SELECT run_id, status, error FROM fr.refresh_run ORDER BY run_id").fetchall()


def published_run(dsn):
    return (read_status(dsn) or {}).get("run_id")


# ---------- happy path ----------
def test_publish_and_read_back(db):
    out = refresh(db["writer"])
    assert out.status == "succeeded" and out.run_id == 1
    st = read_status(db["reader"])
    assert st["run_id"] == 1 and st["snapshot_at"] is not None
    assert st["salesforce_as_of"] and st["databricks_as_of"] and st["snowflake_as_of"]   # freshness on every read
    rows = read_customers(db["reader"], at_risk_only=True)
    assert {r["customer_id"]: r["status"] for r in rows} == EXPECTED["customers_at_risk"]
    assert sum(r["short_value"] for r in read_customers(db["reader"])) == EXPECTED["short_value_usd"]
    assert all(r["snapshot_at"] == st["snapshot_at"] for r in rows)
    lines = read_lines(db["reader"], order_id="SO-9003")
    assert lines[0]["short_qty"] == 5 and lines[0]["status"] == "PARTIAL" and "available" in lines[0]["reason"]


def test_rerun_is_idempotent(db):
    a, b = refresh(db["writer"]), refresh(db["writer"])
    assert (a.status, b.status) == ("succeeded", "succeeded") and published_run(db["reader"]) == b.run_id
    first = read_customers(db["reader"])
    assert [(r["customer_id"], r["status"], r["short_value"]) for r in first] == \
           [(c.customer_id, c.status.value, c.short_value) for c in a.result.customers]


# ---------- failures never overwrite the last good snapshot ----------
@pytest.mark.parametrize("source", ["salesforce", "databricks", "snowflake"])
def test_source_outage_keeps_last_snapshot(db, source):
    good = refresh(db["writer"])
    before = read_customers(db["reader"])
    bad = refresh(db["writer"], Scenario(fail={source}))
    assert bad.status == "failed" and source in bad.error
    assert published_run(db["reader"]) == good.run_id and read_customers(db["reader"]) == before
    assert runs(db["owner"])[-1][1] == "failed"
    assert read_status(db["reader"])["failures_since_snapshot"] == 1


def test_failure_after_rows_written_rolls_back(db):
    good = refresh(db["writer"])
    bad = refresh(db["writer"], _fail_after_rows=True)
    assert bad.status == "failed" and "after writing" in bad.error
    assert published_run(db["reader"]) == good.run_id
    with psycopg.connect(db["owner"]) as c:                       # no orphan rows from the failed run
        assert c.execute("SELECT count(*) FROM fr.customer_risk WHERE run_id=%s", (bad.run_id,)).fetchone()[0] == 0


def test_first_ever_refresh_failing_publishes_nothing(db):
    out = refresh(db["writer"], Scenario(fail={"databricks"}))
    assert out.status == "failed" and read_status(db["reader"]) is None and read_customers(db["reader"]) == []


def test_stale_source_is_not_published(db):
    good = refresh(db["writer"])
    out = refresh(db["writer"], Scenario(age={"snowflake": timedelta(hours=30)}))
    assert out.status == "failed" and "stale" in out.error and "snowflake" in out.error
    assert published_run(db["reader"]) == good.run_id
    loose = refresh(db["writer"], Scenario(age={"snowflake": timedelta(hours=30)}),
                    settings=RefreshSettings(max_age_hours=48))
    assert loose.status == "succeeded"


def test_empty_inventory_or_customers_rejected(db):
    assert "no inventory" in refresh(db["writer"], Scenario(inventory_override=())).error
    assert "no customers" in refresh(db["writer"], Scenario(customers_override=())).error
    assert read_status(db["reader"]) is None


def test_no_open_orders_publishes_empty_result(db):
    out = refresh(db["writer"], Scenario(lines_override=()))
    assert out.status == "succeeded" and read_customers(db["reader"]) == [] and read_status(db["reader"])


def test_too_many_data_issues_rejected(db):
    bad = tuple(line(f"X{i}", ACME, "UNKNOWN-SKU", 1) for i in range(3))   # 3 of 13 lines = 23% > 10%
    out = refresh(db["writer"], Scenario(extra_lines=bad))
    assert out.status == "failed" and "data issues" in out.error
    ok = refresh(db["writer"], Scenario(extra_lines=bad[:1]))             # 1 of 11 = 9% -> published, flagged
    assert ok.status == "succeeded"
    flagged = [r for r in read_customers(db["reader"]) if r["customer_id"] == ACME][0]
    assert flagged["status"] == "DATA_ISSUE"


# ---------- concurrency and crash recovery ----------
def test_second_refresh_while_one_runs_is_skipped(db):
    with psycopg.connect(db["writer"], autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        out = refresh(db["writer"])
        assert out.status == "skipped" and out.run_id is None
    assert refresh(db["writer"]).status == "succeeded"


def test_crashed_run_is_marked_abandoned(db):
    s = SnapshotStore(db["writer"])
    s.begin("test")
    s.conn.close()                      # process "dies": lock released, run left 'running'
    out = refresh(db["writer"])
    assert out.status == "succeeded"
    assert [r[1] for r in runs(db["owner"])] == ["abandoned", "succeeded"]


def test_retention_keeps_published_run(db):
    for _ in range(4):
        refresh(db["writer"])
    good = published_run(db["reader"])
    refresh(db["writer"], Scenario(fail={"snowflake"}))
    refresh(db["writer"], settings=RefreshSettings(retention_runs=2), sc=Scenario(fail={"salesforce"}))
    out = refresh(db["writer"], settings=RefreshSettings(retention_runs=2))
    ids = [r[0] for r in runs(db["owner"])]
    assert len(ids) == 2 and out.run_id in ids and good not in ids
    assert published_run(db["reader"]) == out.run_id


# ---------- access control ----------
def test_reader_can_only_select_views(db):
    refresh(db["writer"])
    with psycopg.connect(db["reader"]) as c:
        for v in ("v_snapshot_status", "v_fulfillment_risk_current", "v_fulfillment_risk_lines_current",
                  "v_sku_risk_current", "v_refresh_history"):
            c.execute(f"SELECT count(*) FROM fr.{v}").fetchone()
    for stmt in ("SELECT * FROM fr.customer_risk", "SELECT * FROM fr.refresh_run",
                 "DELETE FROM fr.published_snapshot", "UPDATE fr.customer_risk SET short_value=0",
                 "INSERT INTO fr.refresh_run (status, code_version) VALUES ('succeeded','x')"):
        with psycopg.connect(db["reader"]) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute(stmt)


def test_writer_cannot_change_schema(db):
    with psycopg.connect(db["writer"]) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute("DROP TABLE fr.customer_risk")


def test_values_are_exact_decimals(db):
    refresh(db["writer"])
    r = [x for x in read_customers(db["reader"]) if x["customer_id"] == INITECH][0]
    assert r["short_value"] == Decimal("17000.00") and isinstance(r["short_value"], Decimal)


def test_cli_exit_codes(db, monkeypatch):
    from fulfillment_risk.cli import main
    monkeypatch.setenv("FR_DATABASE_URL", db["writer"])
    assert main(["refresh"]) == 0
    with psycopg.connect(db["writer"], autocommit=True) as holder:      # overlapping run -> skipped, exit 0
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        assert main(["refresh"]) == 0
    monkeypatch.setenv("FR_MAX_SOURCE_AGE_HOURS", "-1")                  # every source "stale" -> failed, exit 1
    assert main(["refresh"]) == 1


def test_refresh_history_view(db):
    refresh(db["writer"])
    refresh(db["writer"], Scenario(fail={"snowflake"}))
    with psycopg.connect(db["reader"]) as c:
        rows = c.execute("SELECT run_id, status, is_published, customers_at_risk, error FROM fr.v_refresh_history "
                         "ORDER BY run_id").fetchall()
    assert rows[0][:4] == (1, "succeeded", True, 2) and rows[1][1] == "failed" and "snowflake" in rows[1][4]
