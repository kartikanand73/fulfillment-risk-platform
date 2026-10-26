"""Snowflake inventory via the SQL API. Read-only: one SELECT, run as a role that only has SELECT.

  POST {account_url}/api/v2/statements   {statement, warehouse, role, timeout}
       Authorization: Bearer <PAT>   X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN
  202 -> GET /api/v2/statements/{handle} until 200;  more partitions -> GET ...?partition=N
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from ..contracts import InventoryPosition, SourceBatch
from .auth import require_select, secret_from
from .base import SourceError

COLUMNS = ("PRODUCT_SKU", "LOCATION", "ON_HAND", "RESERVED", "READ_AT")


@dataclass(frozen=True)
class SnowflakeConfig:
    account_url: str
    table: str = "SUPPLY_CHAIN_DEMO.INVENTORY.INVENTORY_POSITION"
    warehouse: str = "FR_READER_WH"
    role: str = "FR_READER"
    secret_name: str = "snowflake-fr-pat"
    timeout_s: float = 120.0

    @staticmethod
    def from_env() -> "SnowflakeConfig":
        e = lambda k, d="": os.environ.get(k, d).strip()
        url = e("SNOWFLAKE_ACCOUNT_URL")
        if not url:
            raise SourceError("snowflake", "SNOWFLAKE_ACCOUNT_URL is required")
        if not url.startswith("https://"):
            url = "https://" + url
        return SnowflakeConfig(url.rstrip("/"), e("FR_SNOWFLAKE_TABLE", SnowflakeConfig.table),
                               e("FR_SNOWFLAKE_WAREHOUSE", SnowflakeConfig.warehouse),
                               e("FR_SNOWFLAKE_ROLE", SnowflakeConfig.role),
                               e("FR_SNOWFLAKE_SECRET_NAME", SnowflakeConfig.secret_name))


class SnowflakeInventory:
    name = "snowflake:sqlapi"

    def __init__(self, cfg: SnowflakeConfig, http: httpx.Client | None = None, pat_provider=None, sleep=time.sleep):
        self._cfg = cfg
        self._http = http or httpx.Client(timeout=60)
        self._pat = pat_provider or secret_from(cfg.secret_name, "FR_SNOWFLAKE_PAT")
        self._sleep = sleep

    def _statement(self) -> str:
        return require_select("SELECT PRODUCT_SKU, LOCATION, ON_HAND, RESERVED, "
                              "TO_VARCHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3') "
                              f"AS READ_AT FROM {self._cfg.table}")

    def _req(self, method, path, headers, **kw):
        r = self._http.request(method, self._cfg.account_url + path, headers=headers, **kw)
        if r.status_code not in (200, 202):
            msg = ""
            try:
                msg = str(r.json().get("message", ""))[:200]
            except ValueError:
                pass
            raise SourceError("snowflake", f"HTTP {r.status_code} {msg}".strip())
        return r

    def fetch_inventory(self) -> SourceBatch[InventoryPosition]:
        try:
            h = {"Authorization": f"Bearer {self._pat()}", "Content-Type": "application/json",
                 "Accept": "application/json", "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN"}
            r = self._req("POST", "/api/v2/statements", h, json={
                "statement": self._statement(), "warehouse": self._cfg.warehouse, "role": self._cfg.role,
                "timeout": int(self._cfg.timeout_s)})
            deadline = time.monotonic() + self._cfg.timeout_s
            while r.status_code == 202:
                if time.monotonic() > deadline:
                    raise SourceError("snowflake", "query timed out")
                self._sleep(2)
                r = self._req("GET", f"/api/v2/statements/{r.json()['statementHandle']}", h)
            j = r.json()
            names = [c["name"].upper() for c in j["resultSetMetaData"]["rowType"]]
            if tuple(names) != COLUMNS:
                raise SourceError("snowflake", f"unexpected columns {names}")
            data = list(j.get("data") or [])
            for p in range(1, len(j["resultSetMetaData"].get("partitionInfo") or [1])):
                data += self._req("GET", f"/api/v2/statements/{j['statementHandle']}", h,
                                  params={"partition": p}).json().get("data") or []
            rows, read_at = [], None
            for v in data:
                sku, loc, on_hand, reserved, ts = v
                read_at = read_at or ts
                rows.append(InventoryPosition(sku, loc, int(on_hand), int(reserved or 0)))
            as_of = (datetime.fromisoformat(read_at).replace(tzinfo=timezone.utc) if read_at
                     else datetime.now(timezone.utc))
            return SourceBatch("snowflake", as_of, tuple(rows))
        except SourceError:
            raise
        except httpx.HTTPError as e:
            raise SourceError("snowflake", f"could not connect ({type(e).__name__})") from None
        except (KeyError, ValueError, TypeError) as e:
            raise SourceError("snowflake", f"bad response ({type(e).__name__}: {e})") from None
        except Exception as e:
            raise SourceError("snowflake", f"{type(e).__name__}") from None
