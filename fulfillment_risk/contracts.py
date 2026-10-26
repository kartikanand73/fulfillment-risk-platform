"""Data contract shared by every source adapter, the engine and the store.

Join keys (stable across the three systems of record):
  customer_id  = Salesforce Account Id (18 chars)          Salesforce  -> Databricks orders
  product_sku  = product id used in Databricks orders      Databricks  -> Snowflake inventory

All money is USD as Decimal; all timestamps are timezone-aware UTC.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Generic, TypeVar

T = TypeVar("T")

TIER_RANK = {"Enterprise": 0, "Mid-Market": 1, "SMB": 2}   # lower = served first on ties


class Status(str, Enum):
    FULFILLABLE = "FULFILLABLE"
    PARTIAL = "PARTIAL"
    UNFULFILLABLE = "UNFULFILLABLE"
    DATA_ISSUE = "DATA_ISSUE"


# worst-of ordering used to roll lines up to orders and customers
SEVERITY = {Status.FULFILLABLE: 0, Status.PARTIAL: 1, Status.UNFULFILLABLE: 2, Status.DATA_ISSUE: 3}


@dataclass(frozen=True)
class Customer:
    customer_id: str
    name: str
    tier: str = "SMB"
    owner: str = ""


@dataclass(frozen=True)
class OpenOrderLine:
    order_id: str
    line_number: int
    customer_id: str
    product_sku: str
    quantity: int
    unit_net_value: Decimal          # net value per unit after discount
    order_date: date
    status: str                      # Processing | Shipped (open = not delivered/cancelled/returned)

    @property
    def net_value(self) -> Decimal:
        return self.unit_net_value * self.quantity


@dataclass(frozen=True)
class InventoryPosition:
    product_sku: str
    location: str
    on_hand: int
    reserved: int = 0

    @property
    def available(self) -> int:
        return max(self.on_hand - self.reserved, 0)


@dataclass(frozen=True)
class SourceBatch(Generic[T]):
    """What every source returns: the rows plus when the source says they were current."""
    source: str                      # salesforce | databricks | snowflake (or synthetic:*)
    as_of: datetime
    rows: tuple[T, ...] = field(default_factory=tuple)

    def __post_init__(self):
        if self.as_of.tzinfo is None:
            raise ValueError(f"{self.source}: as_of must be timezone-aware")

    @property
    def row_count(self) -> int:
        return len(self.rows)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
