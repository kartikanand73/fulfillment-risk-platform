"""Signal validation (pure)."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from fulfillment_risk.signals import Invalid, make, parse

NOW = datetime(2026, 10, 26, 22, 0, tzinfo=timezone.utc)
GLOBEX = "001fj000026l3pEAAQ"


def ok_events():
    t = NOW - timedelta(minutes=3)
    return [make("inventory.adjusted", t, product_sku="P-1003", location="WEST-DC", delta=-5, reason="cycle count"),
            make("stock.received", t, product_sku="P-1001", location="EAST-DC", quantity=20),
            make("order.status_changed", t, order_id="SO-101996", customer_id=GLOBEX, from_status="Processing",
                 to_status="OnHold"),
            make("shipment.delayed", t, order_id="SO-101991", customer_id="001fj000026m1nLAAQ",
                 expected_date="2026-11-02", reason="carrier capacity")]


def test_valid_events_parse_with_entity_keys():
    keys = [parse(json.dumps(e).encode(), NOW).entity_key for e in ok_events()]
    assert keys == ["sku:P-1003@WEST-DC", "sku:P-1001@EAST-DC", "order:SO-101996", "order:SO-101991"]
    s = parse(ok_events()[2], NOW)
    assert s.customer_id == GLOBEX and s.order_id == "SO-101996" and s.payload["to_status"] == "OnHold"


@pytest.mark.parametrize("body,reason", [
    (b"{not json", "bad_json"),
    (b"\xff\xfe", "bad_json"),
    (b"[1,2]", "not_object"),
    (b"x" * (33 * 1024), "too_large"),
    (json.dumps({"event_type": "stock.received"}).encode(), "missing_field"),
])
def test_rejections_raw(body, reason):
    with pytest.raises(Invalid) as e:
        parse(body, NOW)
    assert e.value.reason == reason


def mutate(i, **chg):
    e = ok_events()[i]
    e.update(chg)
    for k, v in list(e.items()):
        if v is None:
            del e[k]
    return e


@pytest.mark.parametrize("event,reason", [
    (mutate(0, event_id="not-a-uuid"), "bad_event_id"),
    (mutate(0, event_type="inventory.teleported"), "unknown_type"),
    (mutate(0, occurred_at="yesterday"), "bad_timestamp"),
    (mutate(0, occurred_at="2026-10-26T21:00:00"), "bad_timestamp"),                    # no offset
    (mutate(0, occurred_at=(NOW + timedelta(hours=1)).isoformat()), "future_event"),
    (mutate(0, occurred_at=(NOW - timedelta(days=8)).isoformat()), "stale_event"),
    (mutate(0, delta=0), "bad_field"),
    (mutate(0, delta="5"), "bad_field"),
    (mutate(1, quantity=-3), "bad_field"),
    (mutate(2, order_id="DROP TABLE"), "bad_field"),
    (mutate(2, to_status="Teleported"), "bad_field"),
    (mutate(2, customer_id="acme"), "bad_field"),
    (mutate(3, expected_date="next week"), "bad_field"),
    (mutate(3, reason=None), "missing_field"),
    (mutate(0, source="bad source with spaces"), "bad_source"),
])
def test_rejections_fields(event, reason):
    with pytest.raises(Invalid) as e:
        parse(event, NOW)
    assert e.value.reason == reason
    if reason not in ("bad_event_id",):
        assert e.value.event_id == event["event_id"]          # rejected rows keep the id when there is one


def test_small_clock_skew_allowed():
    assert parse(mutate(0, occurred_at=(NOW + timedelta(minutes=4)).isoformat()), NOW)
