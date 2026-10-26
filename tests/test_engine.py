"""Allocation and classification rules (pure Python, no database)."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from fulfillment_risk.contracts import Customer, InventoryPosition, Status
from fulfillment_risk.engine import check_freshness, compute, money
from fulfillment_risk.sources.synthetic import line

ENT = Customer("C-ENT", "Big Co", "Enterprise")
MID = Customer("C-MID", "Mid Co", "Mid-Market")
SMB = Customer("C-SMB", "Small Co", "SMB")
CUSTS = (ENT, MID, SMB)


def inv(sku, qty, loc="DC1", reserved=0):
    return InventoryPosition(sku, loc, qty, reserved)


def by_order(r):
    return {(l.order_id, l.line_number): l for l in r.lines}


def test_exact_fit_is_fulfillable():
    r = compute(CUSTS, (line("O1", "C-ENT", "S1", 5),), (inv("S1", 5),))
    l = r.lines[0]
    assert (l.status, l.allocated_qty, l.short_qty, l.short_value) == (Status.FULFILLABLE, 5, 0, Decimal("0.00"))
    assert r.customers[0].status == Status.FULFILLABLE and r.summary["customers_at_risk"] == 0


def test_partial_and_short_value():
    r = compute(CUSTS, (line("O1", "C-ENT", "S1", 10, "12.345"),), (inv("S1", 7),))
    l = r.lines[0]
    assert (l.status, l.allocated_qty, l.short_qty) == (Status.PARTIAL, 7, 3)
    assert l.short_value == Decimal("37.04")          # 3 x 12.345 = 37.035 -> half-up
    assert l.unit_net_value == Decimal("12.35")


def test_zero_stock_is_unfulfillable():
    r = compute(CUSTS, (line("O1", "C-ENT", "S1", 2),), (inv("S1", 3, reserved=3),))
    assert r.lines[0].status == Status.UNFULFILLABLE and r.lines[0].sku_available_before == 0


def test_oldest_order_wins():
    lines = (line("O-NEW", "C-ENT", "S1", 5, day=date(2026, 10, 2)),
             line("O-OLD", "C-SMB", "S1", 5, day=date(2026, 10, 1)))
    r = by_order(compute(CUSTS, lines, (inv("S1", 5),)))
    assert r[("O-OLD", 1)].status == Status.FULFILLABLE        # older SMB beats newer Enterprise
    assert r[("O-NEW", 1)].status == Status.UNFULFILLABLE


def test_same_day_tier_then_order_id():
    d = date(2026, 10, 1)
    lines = (line("O-2", "C-SMB", "S1", 4, day=d), line("O-3", "C-ENT", "S1", 4, day=d),
             line("O-1", "C-MID", "S1", 4, day=d), line("O-0", "C-ENT", "S1", 4, day=d))
    r = by_order(compute(CUSTS, lines, (inv("S1", 10),)))
    assert [r[(o, 1)].allocation_rank for o in ("O-0", "O-3", "O-1", "O-2")] == [1, 2, 3, 4]
    assert [r[(o, 1)].allocated_qty for o in ("O-0", "O-3", "O-1", "O-2")] == [4, 4, 2, 0]


def test_locations_are_pooled_and_reserved_excluded():
    r = compute(CUSTS, (line("O1", "C-ENT", "S1", 12),),
                (inv("S1", 5, "EAST"), inv("S1", 10, "WEST", reserved=3)))
    assert r.lines[0].allocated_qty == 12 and r.skus[0].available == 12


def test_unknown_sku_is_data_issue_and_consumes_nothing():
    r = compute(CUSTS, (line("O1", "C-ENT", "NOPE", 3),), (inv("S1", 5),))
    l = r.lines[0]
    assert l.status == Status.DATA_ISSUE and "not in inventory" in l.reason and l.allocated_qty == 0
    assert r.customers[0].status == Status.DATA_ISSUE


def test_unknown_customer_still_consumes_stock():
    d = date(2026, 10, 1)
    lines = (line("O-GHOST", "C-UNKNOWN", "S1", 5, day=d), line("O-REAL", "C-ENT", "S1", 5, day=d + timedelta(1)))
    r = by_order(compute(CUSTS, lines, (inv("S1", 5),)))
    assert r[("O-GHOST", 1)].status == Status.DATA_ISSUE and r[("O-GHOST", 1)].allocated_qty == 5
    assert r[("O-REAL", 1)].status == Status.UNFULFILLABLE


def test_order_and_customer_roll_up_to_worst():
    lines = (line("O1", "C-ENT", "S1", 5, n=1), line("O1", "C-ENT", "S2", 5, n=2), line("O2", "C-ENT", "S1", 1))
    r = compute(CUSTS, lines, (inv("S1", 6), inv("S2", 2)))
    c = r.customers[0]
    assert c.status == Status.PARTIAL and c.at_risk_orders == 1 and c.open_orders == 2 and c.worst_order_id == "O1"
    assert c.blocked_order_value == Decimal("1000.00")          # whole of O1 (10 units x 100) can't ship complete


def test_customers_without_open_orders_are_not_listed():
    r = compute(CUSTS, (line("O1", "C-ENT", "S1", 1),), (inv("S1", 1),))
    assert [c.customer_id for c in r.customers] == ["C-ENT"]


def test_no_open_orders_is_valid():
    r = compute(CUSTS, (), (inv("S1", 1),))
    assert r.customers == () and r.summary["open_lines"] == 0 and r.summary["short_value"] == Decimal("0.00")


def test_duplicate_line_and_bad_quantity_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        compute(CUSTS, (line("O1", "C-ENT", "S1", 1), line("O1", "C-ENT", "S1", 2)), (inv("S1", 5),))
    with pytest.raises(ValueError, match="quantity"):
        compute(CUSTS, (line("O1", "C-ENT", "S1", 0),), (inv("S1", 5),))


def test_input_order_does_not_change_result():
    lines = tuple(line(f"O{i}", ("C-ENT", "C-MID", "C-SMB")[i % 3], f"S{i % 2}", 3 + i % 4,
                       day=date(2026, 10, 1 + i % 5)) for i in range(30))
    stock = (inv("S0", 20), inv("S1", 15, "W"), inv("S1", 5, "E"))
    a = compute(CUSTS, lines, stock)
    b = compute(tuple(reversed(CUSTS)), tuple(reversed(lines)), tuple(reversed(stock)))
    assert a == b


def test_money_rounding_half_up():
    assert money(Decimal("0.005")) == Decimal("0.01") and money(Decimal("2.675")) == Decimal("2.68")


def test_freshness():
    now = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)
    stale = check_freshness({"a": now - timedelta(hours=1), "b": now - timedelta(hours=30),
                             "c": now + timedelta(hours=2)}, now, 24)
    assert stale == ["b (30.0h old)", "c (-2.0h old)"]
