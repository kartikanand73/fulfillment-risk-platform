"""One refresh: fetch the three sources -> validate -> compute -> publish. No model calls on this path.

A refresh PUBLISHES only if every gate passes; otherwise the run is recorded as failed and the previously
published snapshot stays exactly as it was.
  gate 1  every source answered                         (SourceError -> fail)
  gate 2  no source is older than max_age_hours          (stale -> fail)
  gate 3  customers and inventory are not empty          (an empty extract is almost always a broken one)
  gate 4  data-issue lines <= max_data_issue_ratio       (too many unknown SKUs/customers -> fail)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime

from . import __version__
from .contracts import utcnow
from .engine import RiskResult, check_freshness, compute
from .sources.base import CustomerSource, InventorySource, OpenOrderSource, SourceError
from .store import RefreshInProgress, SnapshotStore


class RefreshRejected(Exception):
    """A validation gate failed; nothing was published."""


@dataclass(frozen=True)
class RefreshSettings:
    max_age_hours: float = 24.0
    max_data_issue_ratio: float = 0.10
    retention_runs: int = 50

    @staticmethod
    def from_env() -> "RefreshSettings":
        e = os.environ.get
        return RefreshSettings(float(e("FR_MAX_SOURCE_AGE_HOURS", "24")),
                               float(e("FR_MAX_DATA_ISSUE_RATIO", "0.10")),
                               int(e("FR_RETENTION_RUNS", "50")))


@dataclass
class RefreshOutcome:
    run_id: int | None
    status: str                    # succeeded | failed | skipped
    error: str | None = None
    result: RiskResult | None = None
    sources: dict = field(default_factory=dict)


def run_refresh(dsn: str, customers_src: CustomerSource, orders_src: OpenOrderSource, inventory_src: InventorySource,
                settings: RefreshSettings | None = None, trigger: str = "manual", now: datetime | None = None,
                _fail_after_rows: bool = False) -> RefreshOutcome:
    s = settings or RefreshSettings()
    store = SnapshotStore(dsn)
    try:
        run_id = store.begin(__version__, trigger)
    except RefreshInProgress as e:
        return RefreshOutcome(None, "skipped", str(e))
    sources: dict = {}
    try:
        batches = {}
        for key, src, fn in (("salesforce", customers_src, "fetch_customers"),
                             ("databricks", orders_src, "fetch_open_order_lines"),
                             ("snowflake", inventory_src, "fetch_inventory")):
            b = getattr(src, fn)()
            batches[key] = b
            sources[key] = {"adapter": src.name, "as_of": b.as_of, "rows": b.row_count}
        store.record_sources(sources)

        stale = check_freshness({k: b.as_of for k, b in batches.items()}, now or utcnow(), s.max_age_hours)
        if stale:
            raise RefreshRejected(f"stale source data: {', '.join(stale)} (limit {s.max_age_hours}h)")
        if not batches["salesforce"].rows:
            raise RefreshRejected("salesforce returned no customers")
        if not batches["snowflake"].rows:
            raise RefreshRejected("snowflake returned no inventory")

        result = compute(batches["salesforce"].rows, batches["databricks"].rows, batches["snowflake"].rows)
        n = result.summary["open_lines"]
        ratio = result.summary["data_issue_lines"] / n if n else 0.0
        if ratio > s.max_data_issue_ratio:
            raise RefreshRejected(f"{result.summary['data_issue_lines']} of {n} lines have data issues "
                                  f"({ratio:.0%} > {s.max_data_issue_ratio:.0%})")
        store.publish(result, s.retention_runs, fail_after_rows=_fail_after_rows)
        return RefreshOutcome(run_id, "succeeded", None, result, sources)
    except (SourceError, RefreshRejected, ValueError, RuntimeError) as e:
        store.fail(f"{type(e).__name__}: {e}")
        return RefreshOutcome(run_id, "failed", f"{type(e).__name__}: {e}", None, sources)
    except Exception as e:  # anything unexpected: still record it, never publish
        store.fail(f"{type(e).__name__}: {e}")
        raise
    finally:
        store.close()
