"""PostgreSQL snapshot store.

Guarantees:
  * one refresh at a time      - session advisory lock; a second refresh gets RefreshInProgress
  * failed refresh never wins  - results and the published pointer move in ONE transaction; on any error the
                                 transaction rolls back and the run is marked failed in a separate one
  * every run is recorded      - status, sources (adapter, as_of, rows), summary, error, code version
  * crash recovery             - runs left 'running' by a dead process are marked 'abandoned' by the next refresh
  * bounded history            - keeps the newest N runs (never deletes the published one)
"""
from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

import psycopg

from ..engine import RiskResult

LOCK_KEY = 0x4652_534B   # 'FRSK'
SCHEMA_SQL = Path(__file__).with_name("schema.sql")


class RefreshInProgress(Exception):
    pass


def _json(o):
    if isinstance(o, Decimal):
        return str(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    if hasattr(o, "value"):
        return o.value
    raise TypeError(type(o))


def apply_schema(dsn: str) -> None:
    """Create/upgrade the fr schema (needs a role that can create roles/schemas - run by an admin)."""
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(SCHEMA_SQL.read_text())


class SnapshotStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn: psycopg.Connection | None = None
        self.run_id: int | None = None

    # ---- run lifecycle ----
    def begin(self, code_version: str, trigger: str = "manual") -> int:
        self.conn = psycopg.connect(self.dsn, autocommit=True)
        got = self.conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,)).fetchone()[0]
        if not got:
            self.conn.close()
            self.conn = None
            raise RefreshInProgress("another refresh is running")
        # we hold the lock, so anything still 'running' belongs to a process that died
        self.conn.execute("UPDATE fr.refresh_run SET status='abandoned', finished_at=now(), "
                          "error=coalesce(error, 'process ended before finishing') WHERE status='running'")
        self.run_id = self.conn.execute(
            "INSERT INTO fr.refresh_run (status, trigger, code_version) VALUES ('running', %s, %s) RETURNING run_id",
            (trigger, code_version)).fetchone()[0]
        return self.run_id

    def record_sources(self, sources: dict) -> None:
        self.conn.execute("UPDATE fr.refresh_run SET sources=%s::jsonb WHERE run_id=%s",
                          (json.dumps(sources, default=_json), self.run_id))

    def publish(self, result: RiskResult, retention_runs: int = 50, fail_after_rows: bool = False) -> None:
        """Write results and move the published pointer atomically. `fail_after_rows` is a test hook."""
        rid = self.run_id
        with self.conn.transaction():
            cur = self.conn.cursor()
            cur.executemany(
                "INSERT INTO fr.customer_risk VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(rid, c.customer_id, c.customer_name, c.tier, c.status.value, c.open_orders, c.open_lines,
                  c.at_risk_orders, c.short_units, c.open_value, c.short_value, c.blocked_order_value,
                  c.worst_order_id) for c in result.customers])
            cur.executemany(
                "INSERT INTO fr.order_line_risk VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(rid, l.order_id, l.line_number, l.customer_id, l.customer_name, l.product_sku, l.order_date,
                  l.quantity, l.allocated_qty, l.short_qty, l.unit_net_value, l.line_value, l.short_value,
                  l.allocation_rank, l.sku_available_before, l.status.value, l.reason) for l in result.lines])
            cur.executemany("INSERT INTO fr.sku_risk VALUES (%s,%s,%s,%s,%s,%s)",
                            [(rid, s.product_sku, s.available, s.demanded, s.allocated, s.short_units)
                             for s in result.skus])
            if fail_after_rows:
                raise RuntimeError("simulated failure after writing result rows")
            cur.execute("UPDATE fr.refresh_run SET status='succeeded', finished_at=now(), summary=%s::jsonb "
                        "WHERE run_id=%s", (json.dumps(result.summary, default=_json), rid))
            cur.execute("INSERT INTO fr.published_snapshot (singleton, run_id, published_at) VALUES (true, %s, now()) "
                        "ON CONFLICT (singleton) DO UPDATE SET run_id=EXCLUDED.run_id, published_at=EXCLUDED.published_at",
                        (rid,))
        self._retain(retention_runs)

    def fail(self, error: str) -> None:
        if self.conn and self.run_id:
            self.conn.execute("UPDATE fr.refresh_run SET status='failed', finished_at=now(), error=%s WHERE run_id=%s",
                              (error[:2000], self.run_id))

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
            finally:
                self.conn.close()
                self.conn = None

    def _retain(self, keep: int) -> None:
        self.conn.execute(
            "DELETE FROM fr.refresh_run WHERE run_id NOT IN (SELECT run_id FROM fr.refresh_run ORDER BY run_id DESC "
            "LIMIT %s) AND run_id <> (SELECT run_id FROM fr.published_snapshot)", (keep,))


# ---- read side (what the agent skill will use, as fr_reader) ----
def read_status(dsn: str) -> dict | None:
    with psycopg.connect(dsn) as c:
        cur = c.cursor(row_factory=psycopg.rows.dict_row)
        return cur.execute("SELECT * FROM fr.v_snapshot_status").fetchone()


def read_customers(dsn: str, at_risk_only: bool = False) -> list[dict]:
    q = "SELECT * FROM fr.v_fulfillment_risk_current"
    if at_risk_only:
        q += " WHERE status <> 'FULFILLABLE'"
    q += " ORDER BY short_value DESC, customer_id"
    with psycopg.connect(dsn) as c:
        return c.cursor(row_factory=psycopg.rows.dict_row).execute(q).fetchall()


def read_lines(dsn: str, customer_id: str | None = None, order_id: str | None = None) -> list[dict]:
    q, args = "SELECT * FROM fr.v_fulfillment_risk_lines_current WHERE true", []
    if customer_id:
        q, args = q + " AND customer_id = %s", args + [customer_id]
    if order_id:
        q, args = q + " AND order_id = %s", args + [order_id]
    with psycopg.connect(dsn) as c:
        return c.cursor(row_factory=psycopg.rows.dict_row).execute(q + " ORDER BY order_id, line_number",
                                                                     args).fetchall()


__all__ = ["SnapshotStore", "RefreshInProgress", "apply_schema", "read_status", "read_customers", "read_lines", "asdict"]
