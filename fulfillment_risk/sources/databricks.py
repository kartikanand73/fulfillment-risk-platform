"""Databricks open order lines via Databricks SQL (Statement Execution API) - deliberately NOT Genie, so the
dataset is deterministic. Read-only: one SELECT on a read-only view.

  POST {host}/api/2.0/sql/statements {warehouse_id, statement, wait_timeout, format: JSON_ARRAY, disposition: INLINE}
  GET  {host}/api/2.0/sql/statements/{id}                      (poll while PENDING/RUNNING)
  GET  {host}{next_chunk_internal_link}                         (further result chunks)
Auth: Entra token for the AzureDatabricks resource (job managed identity when hosted, `az login` locally).
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import httpx

from ..contracts import OpenOrderLine, SourceBatch
from .auth import entra_token_provider, require_select
from .base import SourceError

DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
COLUMNS = ("order_id", "line_number", "customer_id", "product_sku", "quantity", "unit_net_value", "order_date",
           "status", "read_at")


@dataclass(frozen=True)
class DatabricksConfig:
    host: str
    warehouse_id: str
    view: str = "skillagent.sales.v_open_order_lines"
    timeout_s: float = 300.0

    @staticmethod
    def from_env() -> "DatabricksConfig":
        e = lambda k, d="": os.environ.get(k, d).strip()
        host, wh = e("DATABRICKS_HOST"), e("DATABRICKS_WAREHOUSE_ID")
        if not host or not wh:
            raise SourceError("databricks", "DATABRICKS_HOST and DATABRICKS_WAREHOUSE_ID are required")
        if not host.startswith("https://"):
            host = "https://" + host
        return DatabricksConfig(host.rstrip("/"), wh, e("FR_DATABRICKS_VIEW", "skillagent.sales.v_open_order_lines"))


class DatabricksOpenOrders:
    name = "databricks:sql"

    def __init__(self, cfg: DatabricksConfig, http: httpx.Client | None = None, token_provider=None, sleep=time.sleep):
        self._cfg = cfg
        self._http = http or httpx.Client(timeout=60)
        self._tp = token_provider
        self._sleep = sleep

    def _statement(self) -> str:
        cols = ", ".join(COLUMNS[:-1])
        return require_select(f"SELECT {cols}, current_timestamp() AS read_at FROM {self._cfg.view}")

    def _get(self, method, path, **kw):
        if self._tp is None:
            self._tp = entra_token_provider(DATABRICKS_SCOPE)
        r = self._http.request(method, self._cfg.host + path, headers={"Authorization": f"Bearer {self._tp()}"}, **kw)
        if r.status_code != 200:
            msg = ""
            try:
                msg = str(r.json().get("message", ""))[:200]
            except ValueError:
                pass
            raise SourceError("databricks", f"HTTP {r.status_code} {msg}".strip())
        return r.json()

    def fetch_open_order_lines(self) -> SourceBatch[OpenOrderLine]:
        try:
            r = self._get("POST", "/api/2.0/sql/statements", json={
                "warehouse_id": self._cfg.warehouse_id, "statement": self._statement(), "wait_timeout": "50s",
                "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE"})
            deadline = time.monotonic() + self._cfg.timeout_s
            while r["status"]["state"] in ("PENDING", "RUNNING"):
                if time.monotonic() > deadline:
                    raise SourceError("databricks", "query timed out")
                self._sleep(2)
                r = self._get("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
            if r["status"]["state"] != "SUCCEEDED":
                err = (r["status"].get("error") or {}).get("message", r["status"]["state"])
                raise SourceError("databricks", f"query {r['status']['state']}: {str(err)[:200]}")
            names = [c["name"] for c in r["manifest"]["schema"]["columns"]]
            if tuple(names) != COLUMNS:
                raise SourceError("databricks", f"unexpected columns {names}")
            data, chunk = list((r.get("result") or {}).get("data_array") or []), r.get("result") or {}
            while chunk.get("next_chunk_internal_link"):
                chunk = self._get("GET", chunk["next_chunk_internal_link"])
                data += chunk.get("data_array") or []
            rows, read_at = [], None
            for v in data:
                rec = dict(zip(names, v))
                read_at = read_at or rec["read_at"]
                rows.append(OpenOrderLine(
                    rec["order_id"], int(rec["line_number"]), rec["customer_id"], rec["product_sku"],
                    int(rec["quantity"]), Decimal(rec["unit_net_value"]), date.fromisoformat(rec["order_date"]),
                    rec["status"]))
            as_of = _parse_ts(read_at) if read_at else datetime.now(timezone.utc)
            return SourceBatch("databricks", as_of, tuple(rows))
        except SourceError:
            raise
        except httpx.HTTPError as e:
            raise SourceError("databricks", f"could not connect ({type(e).__name__})") from None
        except (KeyError, ValueError, TypeError) as e:
            raise SourceError("databricks", f"bad response ({type(e).__name__}: {e})") from None
        except Exception as e:
            raise SourceError("databricks", f"{type(e).__name__}") from None


def _parse_ts(s: str) -> datetime:
    t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
