"""Synthetic adapters that mirror the real systems (same Salesforce Account Ids, same Databricks SKUs).

The default dataset is hand-built so the answer is known in advance (see EXPECTED below and
docs/data-contract.md). `Scenario` switches let tests inject failures and bad data without touching code.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from ..contracts import Customer, InventoryPosition, OpenOrderLine, SourceBatch
from .base import SourceError

ACME, GLOBEX, INITECH = "001fj000026lzjWAAQ", "001fj000026l3pEAAQ", "001fj000026m1nLAAQ"
UMBRELLA, HOOLI = "001SYNTHUMBRELLAAA", "001SYNTHHOOLI00AAA"   # synthetic extras (not in the real org)

CUSTOMERS = (
    Customer(ACME, "Acme Corp", "Enterprise", "Kartik Anand"),
    Customer(GLOBEX, "Globex Inc", "Enterprise", "Kartik Anand"),
    Customer(INITECH, "Initech", "Mid-Market", "Kartik Anand"),
    Customer(UMBRELLA, "Umbrella Supply (synthetic)", "SMB", "Kartik Anand"),
    Customer(HOOLI, "Hooli (synthetic, no open orders)", "Mid-Market", "Kartik Anand"),
)

D = Decimal
# order_id, line, customer, sku, qty, unit net value, order date
_LINES = (
    ("SO-9001", 1, ACME, "P-1002", 10, D("2160.00"), date(2026, 10, 1)),     # Edge Gateway
    ("SO-9001", 2, ACME, "P-4001", 100, D("45.00"), date(2026, 10, 1)),      # Replacement Battery
    ("SO-9002", 1, GLOBEX, "P-1002", 12, D("2160.00"), date(2026, 10, 3)),
    ("SO-9002", 2, GLOBEX, "P-1003", 20, D("882.00"), date(2026, 10, 3)),    # Rugged Tablet
    ("SO-9003", 1, INITECH, "P-1002", 8, D("2400.00"), date(2026, 10, 3)),   # same day as Globex; Enterprise first
    ("SO-9004", 1, ACME, "P-1003", 25, D("931.00"), date(2026, 10, 5)),
    ("SO-9005", 1, INITECH, "P-1001", 4, D("1250.00"), date(2026, 10, 8)),   # Industrial Sensor Kit (no stock)
    ("SO-9006", 1, GLOBEX, "P-3001", 5, D("1800.00"), date(2026, 10, 10)),   # Implementation day
    ("SO-9006", 2, GLOBEX, "P-2002", 50, D("540.00"), date(2026, 10, 10)),   # Field Service seat
    ("SO-9007", 1, UMBRELLA, "P-4001", 50, D("45.00"), date(2026, 10, 12)),
)
OPEN_LINES = tuple(OpenOrderLine(o, n, c, s, q, v, d, "Processing") for o, n, c, s, q, v, d in _LINES)

INVENTORY = (
    InventoryPosition("P-1001", "EAST-DC", on_hand=6, reserved=6),      # all reserved -> 0 available
    InventoryPosition("P-1002", "EAST-DC", on_hand=20, reserved=0),
    InventoryPosition("P-1002", "WEST-DC", on_hand=10, reserved=5),     # 20 + 5 = 25 available
    InventoryPosition("P-1003", "WEST-DC", on_hand=40, reserved=0),
    InventoryPosition("P-2002", "DIGITAL", on_hand=10_000, reserved=0),
    InventoryPosition("P-3001", "SERVICES", on_hand=100, reserved=10),
    InventoryPosition("P-4001", "EAST-DC", on_hand=500, reserved=0),
)

# The answer the engine must produce for the default dataset (asserted in tests/test_scenario.py).
EXPECTED = {
    "customers_at_risk": {ACME: "PARTIAL", INITECH: "UNFULFILLABLE"},
    "customers_ok": {GLOBEX: "FULFILLABLE", UMBRELLA: "FULFILLABLE"},
    "short_value_usd": D("21655.00"),        # Initech SO-9003 5x2400 + SO-9005 4x1250 + Acme SO-9004 5x931
}


@dataclass
class Scenario:
    """Switches for tests. Defaults = the clean demo dataset."""
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))   # sources report "5 min ago"
    age: dict = field(default_factory=dict)            # source -> timedelta (how old its as_of is)
    fail: set = field(default_factory=set)              # sources that raise SourceError
    extra_lines: tuple = ()                             # additional OpenOrderLine rows
    inventory_override: tuple | None = None
    customers_override: tuple | None = None
    lines_override: tuple | None = None
    shuffle: bool = False                               # reverse row order (engine must not care)


class _Base:
    name = "synthetic"

    def __init__(self, scenario: Scenario | None = None):
        self.sc = scenario or Scenario()

    def _batch(self, source: str, rows) -> SourceBatch:
        if source in self.sc.fail:
            raise SourceError(source, "simulated outage")
        rows = tuple(rows)
        if self.sc.shuffle:
            rows = tuple(reversed(rows))
        return SourceBatch(source, self.sc.now - self.sc.age.get(source, timedelta(minutes=5)), rows)


class SyntheticSalesforce(_Base):
    name = "synthetic:salesforce"

    def fetch_customers(self):
        return self._batch("salesforce", self.sc.customers_override if self.sc.customers_override is not None
                           else CUSTOMERS)


class SyntheticDatabricks(_Base):
    name = "synthetic:databricks"

    def fetch_open_order_lines(self):
        base = self.sc.lines_override if self.sc.lines_override is not None else OPEN_LINES
        return self._batch("databricks", tuple(base) + tuple(self.sc.extra_lines))


class SyntheticSnowflake(_Base):
    name = "synthetic:snowflake"

    def fetch_inventory(self):
        return self._batch("snowflake", self.sc.inventory_override if self.sc.inventory_override is not None
                           else INVENTORY)


def synthetic_sources(scenario: Scenario | None = None):
    sc = scenario or Scenario()
    return SyntheticSalesforce(sc), SyntheticDatabricks(sc), SyntheticSnowflake(sc)


def line(order_id, customer, sku, qty, value="100.00", day=date(2026, 10, 20), n=1) -> OpenOrderLine:
    """Helper for tests."""
    return OpenOrderLine(order_id, n, customer, sku, qty, D(value), day, "Processing")


__all__ = ["Scenario", "synthetic_sources", "line", "EXPECTED", "CUSTOMERS", "OPEN_LINES", "INVENTORY",
           "ACME", "GLOBEX", "INITECH", "UMBRELLA", "HOOLI", "replace"]
