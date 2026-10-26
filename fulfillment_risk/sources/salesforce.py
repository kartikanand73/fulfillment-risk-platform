"""Salesforce customers via the REST query API (SOQL). Read-only: one GET query, no DML.

  POST {my_domain}/services/oauth2/token  grant_type=client_credentials  -> access_token, instance_url
  GET  {instance_url}/services/data/v62.0/query?q=SELECT ... FROM Account   (+ nextRecordsUrl paging)

Tier (used only to break same-day allocation ties): from NumberOfEmployees by default
(>= 1000 Enterprise, >= 100 Mid-Market, else SMB), or from a field named in SALESFORCE_TIER_FIELD whose values
are already 'Enterprise' / 'Mid-Market' / 'SMB'.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

from ..contracts import Customer, SourceBatch, utcnow
from .auth import secret_from
from .base import SourceError

API = "v62.0"


@dataclass(frozen=True)
class SalesforceConfig:
    my_domain: str
    client_id: str
    secret_name: str = "salesforce-client-secret"
    tier_field: str = ""             # optional custom field holding the tier label
    where: str = ""                  # optional extra SOQL filter, e.g. "Type = 'Customer'"
    timeout_s: float = 60.0

    @staticmethod
    def from_env() -> "SalesforceConfig":
        e = lambda k, d="": os.environ.get(k, d).strip()
        dom = e("SALESFORCE_MY_DOMAIN")
        if not dom or not e("SALESFORCE_CLIENT_ID"):
            raise SourceError("salesforce", "SALESFORCE_MY_DOMAIN and SALESFORCE_CLIENT_ID are required")
        if not dom.startswith("https://"):
            dom = "https://" + dom
        return SalesforceConfig(dom.rstrip("/"), e("SALESFORCE_CLIENT_ID"),
                                e("SALESFORCE_SECRET_NAME", "salesforce-client-secret"),
                                e("SALESFORCE_TIER_FIELD"), e("SALESFORCE_ACCOUNT_FILTER"))


def tier_from_employees(n) -> str:
    try:
        n = int(n or 0)
    except (TypeError, ValueError):
        n = 0
    return "Enterprise" if n >= 1000 else "Mid-Market" if n >= 100 else "SMB"


class SalesforceCustomers:
    name = "salesforce:soql"

    def __init__(self, cfg: SalesforceConfig, http: httpx.Client | None = None, secret_provider=None, clock=None):
        self._cfg = cfg
        self._clock = clock or utcnow
        self._http = http or httpx.Client(timeout=cfg.timeout_s)
        self._secret = secret_provider or secret_from(cfg.secret_name, "SALESFORCE_CLIENT_SECRET")

    def _soql(self) -> str:
        fields = ["Id", "Name", "NumberOfEmployees", "Owner.Name"] + ([self._cfg.tier_field] if self._cfg.tier_field else [])
        q = f"SELECT {', '.join(fields)} FROM Account"
        if self._cfg.where:
            q += f" WHERE {self._cfg.where}"
        return q + " ORDER BY Id"

    def fetch_customers(self) -> SourceBatch[Customer]:
        try:
            tok = self._http.post(f"{self._cfg.my_domain}/services/oauth2/token", data={
                "grant_type": "client_credentials", "client_id": self._cfg.client_id,
                "client_secret": self._secret()})
            if tok.status_code != 200:
                raise SourceError("salesforce", f"token request failed (HTTP {tok.status_code})")
            t = tok.json()
            base, h = t.get("instance_url") or self._cfg.my_domain, {"Authorization": f"Bearer {t['access_token']}"}
            as_of = self._clock()
            r = self._http.get(f"{base}/services/data/{API}/query", params={"q": self._soql()}, headers=h)
            rows: list[Customer] = []
            while True:
                if r.status_code != 200:
                    msg = ""
                    try:
                        j = r.json()
                        msg = (j[0] if isinstance(j, list) else j).get("message", "")[:200]
                    except ValueError:
                        pass
                    raise SourceError("salesforce", f"query failed (HTTP {r.status_code}) {msg}".strip())
                j = r.json()
                for rec in j.get("records", []):
                    tier = rec.get(self._cfg.tier_field) if self._cfg.tier_field else None
                    rows.append(Customer(rec["Id"], rec.get("Name") or "", tier or tier_from_employees(
                        rec.get("NumberOfEmployees")), ((rec.get("Owner") or {}).get("Name")) or ""))
                if j.get("done", True) or not j.get("nextRecordsUrl"):
                    break
                r = self._http.get(base + j["nextRecordsUrl"], headers=h)
            return SourceBatch("salesforce", as_of, tuple(rows))
        except SourceError:
            raise
        except httpx.HTTPError as e:
            raise SourceError("salesforce", f"could not connect ({type(e).__name__})") from None
        except Exception as e:  # credential problems etc. - never include secret material
            raise SourceError("salesforce", f"{type(e).__name__}") from None
