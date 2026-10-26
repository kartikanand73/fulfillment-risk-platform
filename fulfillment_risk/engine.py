"""Deterministic fulfillment-risk calculation. No model, no I/O: inputs in, results out.

Allocation rule (documented in docs/decisions.md, D-002):
  available stock per SKU (sum over locations of on_hand - reserved) is allocated to open order lines in
  priority order: order_date ASC, customer tier (Enterprise, Mid-Market, SMB, unknown last),
  order_id ASC, line_number ASC.  Same inputs in any order -> same result.

Line status:
  FULFILLABLE    allocated == quantity
  PARTIAL        0 < allocated < quantity
  UNFULFILLABLE  allocated == 0 (SKU is known, but nothing left for this line)
  DATA_ISSUE     SKU missing from the inventory feed, or customer missing from Salesforce.
                 Unknown-SKU lines consume nothing. Unknown-customer lines are real orders, so they still
                 consume stock in their priority slot (otherwise other customers would look safer than they are).
Order / customer status = the worst line status (DATA_ISSUE > UNFULFILLABLE > PARTIAL > FULFILLABLE).
Money: short_value = short_qty * unit_net_value, rounded half-up to cents.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from .contracts import (SEVERITY, TIER_RANK, Customer, InventoryPosition, OpenOrderLine, Status)

CENT = Decimal("0.01")


def money(x: Decimal) -> Decimal:
    return Decimal(x).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class LineResult:
    order_id: str
    line_number: int
    customer_id: str
    customer_name: str | None
    product_sku: str
    order_date: object
    quantity: int
    allocated_qty: int
    short_qty: int
    unit_net_value: Decimal
    line_value: Decimal
    short_value: Decimal
    allocation_rank: int | None
    sku_available_before: int | None
    status: Status
    reason: str


@dataclass(frozen=True)
class CustomerResult:
    customer_id: str
    customer_name: str | None
    tier: str | None
    open_orders: int
    open_lines: int
    at_risk_orders: int
    short_units: int
    open_value: Decimal
    short_value: Decimal
    blocked_order_value: Decimal      # total value of orders that cannot ship complete
    status: Status
    worst_order_id: str | None


@dataclass(frozen=True)
class SkuResult:
    product_sku: str
    available: int
    demanded: int
    allocated: int
    short_units: int


@dataclass(frozen=True)
class RiskResult:
    lines: tuple[LineResult, ...]
    customers: tuple[CustomerResult, ...]
    skus: tuple[SkuResult, ...]
    summary: dict


def _worst(statuses) -> Status:
    return max(statuses, key=lambda s: SEVERITY[s])


def compute(customers: tuple[Customer, ...], lines: tuple[OpenOrderLine, ...],
            inventory: tuple[InventoryPosition, ...]) -> RiskResult:
    cust = {c.customer_id: c for c in customers}
    available: dict[str, int] = defaultdict(int)
    for p in inventory:
        available[p.product_sku] += p.available
    remaining = dict(available)

    def priority(l: OpenOrderLine):
        c = cust.get(l.customer_id)
        tier = TIER_RANK.get(c.tier, 9) if c else 10
        return (l.order_date, tier, l.order_id, l.line_number)

    seen = set()
    for l in lines:
        k = (l.order_id, l.line_number)
        if k in seen:
            raise ValueError(f"duplicate order line {k}")
        if l.quantity <= 0:
            raise ValueError(f"non-positive quantity on {k}")
        seen.add(k)

    results: list[LineResult] = []
    demanded: dict[str, int] = defaultdict(int)
    allocated_by_sku: dict[str, int] = defaultdict(int)
    for rank, l in enumerate(sorted(lines, key=priority), start=1):
        c = cust.get(l.customer_id)
        sku_known = l.product_sku in available
        before = remaining.get(l.product_sku) if sku_known else None
        if sku_known:
            demanded[l.product_sku] += l.quantity
            alloc = min(l.quantity, remaining[l.product_sku])
            remaining[l.product_sku] -= alloc
            allocated_by_sku[l.product_sku] += alloc
        else:
            alloc = 0
        short = l.quantity - alloc
        if not sku_known:
            status, reason = Status.DATA_ISSUE, f"SKU {l.product_sku} not in inventory feed"
        elif c is None:
            status, reason = Status.DATA_ISSUE, f"customer {l.customer_id} not in Salesforce"
        elif short == 0:
            status, reason = Status.FULFILLABLE, "fully allocated"
        elif alloc > 0:
            status, reason = Status.PARTIAL, f"{alloc} of {l.quantity} allocated; {before} available before this line"
        else:
            status, reason = Status.UNFULFILLABLE, f"no {l.product_sku} left ({before} available before this line)"
        results.append(LineResult(
            l.order_id, l.line_number, l.customer_id, c.name if c else None, l.product_sku, l.order_date,
            l.quantity, alloc, short, money(l.unit_net_value), money(l.net_value),
            money(l.unit_net_value * short), rank, before, status, reason))

    # roll up: orders -> customers
    by_order: dict[str, list[LineResult]] = defaultdict(list)
    for r in results:
        by_order[r.order_id].append(r)
    by_cust: dict[str, list[str]] = defaultdict(list)
    for oid, rs in by_order.items():
        by_cust[rs[0].customer_id].append(oid)

    custs: list[CustomerResult] = []
    for cid, oids in by_cust.items():
        c = cust.get(cid)
        rs = [r for o in oids for r in by_order[o]]
        order_status = {o: _worst(r.status for r in by_order[o]) for o in oids}
        at_risk = [o for o, s in order_status.items() if s != Status.FULFILLABLE]
        worst = max(sorted(oids), key=lambda o: (SEVERITY[order_status[o]],
                                                 sum(r.short_value for r in by_order[o])))
        custs.append(CustomerResult(
            cid, c.name if c else None, c.tier if c else None, len(oids), len(rs), len(at_risk),
            sum(r.short_qty for r in rs), money(sum(r.line_value for r in rs)),
            money(sum(r.short_value for r in rs)),
            money(sum(r.line_value for o in at_risk for r in by_order[o])),
            _worst(r.status for r in rs), worst if at_risk else None))
    custs.sort(key=lambda c: (-SEVERITY[c.status], -c.short_value, c.customer_id))

    skus = tuple(SkuResult(s, available[s], demanded[s], allocated_by_sku[s], demanded[s] - allocated_by_sku[s])
                 for s in sorted(demanded))
    results.sort(key=lambda r: (r.customer_id, r.order_id, r.line_number))
    summary = {
        "customers_with_open_orders": len(custs),
        "customers_at_risk": sum(1 for c in custs if c.status != Status.FULFILLABLE),
        "open_orders": len(by_order),
        "at_risk_orders": sum(c.at_risk_orders for c in custs),
        "open_lines": len(results),
        "data_issue_lines": sum(1 for r in results if r.status == Status.DATA_ISSUE),
        "short_units": sum(r.short_qty for r in results),
        "short_value": money(sum((r.short_value for r in results), Decimal(0))),
        "open_value": money(sum((r.line_value for r in results), Decimal(0))),
        "skus_short": sum(1 for s in skus if s.short_units > 0),
    }
    return RiskResult(tuple(results), tuple(custs), skus, summary)


def check_freshness(as_of: dict[str, datetime], now: datetime, max_age_hours: float) -> list[str]:
    """Names of sources whose data is older than the allowed age (or from the future)."""
    stale = []
    for src, t in as_of.items():
        age_h = (now - t).total_seconds() / 3600
        if age_h > max_age_hours or age_h < -0.25:
            stale.append(f"{src} ({age_h:.1f}h old)")
    return stale
