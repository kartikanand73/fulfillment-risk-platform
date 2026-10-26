"""Operational signal contract (v1) and validation. Pure Python, no I/O.

A signal is CONTEXT, not a source of truth: it is shown on the dashboard immediately but never feeds the
deterministic fulfillment-risk calculation. The next hourly refresh from the systems of record stays authoritative.

Envelope (JSON object, UTF-8, <= 32 KB):
  event_id     UUID                 idempotency key (duplicates are stored once)
  event_type   one of EVENT_TYPES
  occurred_at  ISO-8601 with offset when it happened at the source (used for out-of-order detection)
  source       short producer name
  + the keys and fields each type requires (below)
"""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

MAX_BYTES = 32 * 1024
MAX_FUTURE = timedelta(minutes=5)
MAX_AGE = timedelta(days=7)
ORDER_STATUSES = {"Processing", "OnHold", "Shipped", "Delivered", "Cancelled", "Returned"}

RE_ORDER = re.compile(r"^[A-Z]{2}-\d{3,10}$")
RE_SKU = re.compile(r"^[A-Z0-9][A-Z0-9-]{0,39}$")
RE_CUSTOMER = re.compile(r"^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$")     # Salesforce Account Id (15 or 18)
RE_LOCATION = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,39}$")
RE_SOURCE = re.compile(r"^[A-Za-z0-9:._-]{1,64}$")

# type -> (required fields with validators, entity key builder)
EVENT_TYPES = {
    "inventory.adjusted": {"product_sku": "sku", "location": "location", "delta": "nonzero_int", "reason": "text"},
    "stock.received": {"product_sku": "sku", "location": "location", "quantity": "pos_int"},
    "order.status_changed": {"order_id": "order", "customer_id": "customer", "from_status": "status",
                             "to_status": "status"},
    "shipment.delayed": {"order_id": "order", "customer_id": "customer", "expected_date": "date", "reason": "text"},
}
KEY_FIELDS = ("customer_id", "order_id", "product_sku", "location")


class Invalid(Exception):
    def __init__(self, reason: str, detail: str = "", event_id: str | None = None):
        super().__init__(f"{reason}: {detail}")
        self.reason, self.detail, self.event_id = reason, detail, event_id


@dataclass(frozen=True)
class Signal:
    event_id: str
    event_type: str
    occurred_at: datetime
    source: str
    entity_key: str
    customer_id: str | None = None
    order_id: str | None = None
    product_sku: str | None = None
    location: str | None = None
    payload: dict = field(default_factory=dict)


def entity_key(event_type: str, f: dict) -> str:
    """Out-of-order detection is per entity: an order, or a SKU at a location. Also the Event Hubs partition key,
    so one entity's events stay in order within a partition."""
    if event_type in ("order.status_changed", "shipment.delayed"):
        return f"order:{f['order_id']}"
    return f"sku:{f['product_sku']}@{f['location']}"


def _check(kind: str, name: str, v):
    def bad(msg):
        raise Invalid("bad_field", f"{name}: {msg}")
    if kind == "sku" and not (isinstance(v, str) and RE_SKU.match(v)):
        bad("expected a SKU like P-1003")
    if kind == "location" and not (isinstance(v, str) and RE_LOCATION.match(v)):
        bad("expected a location code like EAST-DC")
    if kind == "order" and not (isinstance(v, str) and RE_ORDER.match(v)):
        bad("expected an order id like SO-101996")
    if kind == "customer" and not (isinstance(v, str) and RE_CUSTOMER.match(v)):
        bad("expected a Salesforce Account Id")
    if kind == "status" and v not in ORDER_STATUSES:
        bad(f"expected one of {sorted(ORDER_STATUSES)}")
    if kind == "nonzero_int" and not (isinstance(v, int) and not isinstance(v, bool) and v != 0 and abs(v) <= 1_000_000):
        bad("expected a non-zero integer")
    if kind == "pos_int" and not (isinstance(v, int) and not isinstance(v, bool) and 0 < v <= 1_000_000):
        bad("expected a positive integer")
    if kind == "text" and not (isinstance(v, str) and 0 < len(v) <= 200):
        bad("expected text (1-200 chars)")
    if kind == "date":
        try:
            date.fromisoformat(v)
        except (TypeError, ValueError):
            bad("expected YYYY-MM-DD")
    return v


def parse(body: bytes | str | dict, now: datetime) -> Signal:
    """Return a valid Signal or raise Invalid(reason). Reasons: too_large, bad_json, not_object, missing_field,
    bad_event_id, unknown_type, bad_timestamp, future_event, stale_event, bad_field, bad_source."""
    if isinstance(body, (bytes, bytearray)):
        if len(body) > MAX_BYTES:
            raise Invalid("too_large", f"{len(body)} bytes")
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError:
            raise Invalid("bad_json", "not UTF-8") from None
    if isinstance(body, str):
        if len(body.encode()) > MAX_BYTES:
            raise Invalid("too_large", f"{len(body)} chars")
        try:
            body = json.loads(body)
        except json.JSONDecodeError as e:
            raise Invalid("bad_json", str(e)[:120]) from None
    if not isinstance(body, dict):
        raise Invalid("not_object", type(body).__name__)
    eid = body.get("event_id")
    for k in ("event_id", "event_type", "occurred_at", "source"):
        if body.get(k) in (None, ""):
            raise Invalid("missing_field", k, eid if isinstance(eid, str) else None)
    try:
        eid = str(uuid.UUID(str(eid)))
    except ValueError:
        raise Invalid("bad_event_id", str(eid)[:60]) from None
    et = body["event_type"]
    if et not in EVENT_TYPES:
        raise Invalid("unknown_type", str(et)[:60], eid)
    if not (isinstance(body["source"], str) and RE_SOURCE.match(body["source"])):
        raise Invalid("bad_source", str(body["source"])[:60], eid)
    try:
        ts = datetime.fromisoformat(str(body["occurred_at"]).replace("Z", "+00:00"))
    except ValueError:
        raise Invalid("bad_timestamp", str(body["occurred_at"])[:60], eid) from None
    if ts.tzinfo is None:
        raise Invalid("bad_timestamp", "occurred_at needs a UTC offset", eid)
    ts = ts.astimezone(timezone.utc)
    if ts > now + MAX_FUTURE:
        raise Invalid("future_event", ts.isoformat(), eid)
    if ts < now - MAX_AGE:
        raise Invalid("stale_event", ts.isoformat(), eid)
    fields = {}
    for name, kind in EVENT_TYPES[et].items():
        if body.get(name) in (None, ""):
            raise Invalid("missing_field", name, eid)
        try:
            fields[name] = _check(kind, name, body[name])
        except Invalid as e:
            e.event_id = eid
            raise
    extra = {k: v for k, v in body.items() if k not in ("event_id", "event_type", "occurred_at", "source")}
    keys = {k: fields.get(k, body.get(k) if isinstance(body.get(k), str) else None) for k in KEY_FIELDS}
    if keys["customer_id"] is not None and not RE_CUSTOMER.match(keys["customer_id"]):
        raise Invalid("bad_field", "customer_id: expected a Salesforce Account Id", eid)
    return Signal(eid, et, ts, body["source"], entity_key(et, fields), payload=extra, **keys)


def make(event_type: str, occurred_at: datetime, source: str = "synthetic", event_id: str | None = None,
         **fields) -> dict:
    """Build a well-formed event body (used by the publisher and tests)."""
    return {"event_id": event_id or str(uuid.uuid4()), "event_type": event_type,
            "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
            "source": source, **fields}
