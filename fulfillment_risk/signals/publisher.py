"""Synthetic, passwordless publisher. Signs in with DefaultAzureCredential (your `az login` locally) and needs only
`Azure Event Hubs Data Sender` on the hub. Valid events use the entity key as partition key, so one order's (or one
SKU@location's) events stay in order within a partition - out-of-order cases are produced deliberately.

usage:
  python -m fulfillment_risk.signals publish --scenario all [--tag t1]
  scenarios: normal | duplicates | out-of-order | invalid | burst | all
Settings: FR_EVENTHUB_NAMESPACE (e.g. evhns-fr-ka2026.servicebus.windows.net), FR_EVENTHUB_NAME (fr-signals)
"""
from __future__ import annotations

import json
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .contract import entity_key, make

GLOBEX, INITECH, ACME = "001fj000026l3pEAAQ", "001fj000026m1nLAAQ", "001fj000026lzjWAAQ"


@dataclass
class Planned:
    """One send: the body bytes, its partition key, and what the pipeline should do with it."""
    body: bytes
    partition_key: str | None
    expect: str                  # accepted | duplicate | late | rejected
    event_id: str | None = None


@dataclass
class Manifest:
    tag: str
    sends: list[list[Planned]] = field(default_factory=list)      # each inner list = one Event Hubs batch

    def expected(self) -> dict:
        out = {"accepted": 0, "duplicate": 0, "late": 0, "rejected": 0}
        for b in self.sends:
            for p in b:
                out[p.expect] += 1
        out["accepted"] += out["late"]          # late events are accepted (and flagged)
        return out

    def event_ids(self) -> set[str]:
        return {p.event_id for b in self.sends for p in b if p.event_id}


def _valid(ev: dict, expect="accepted") -> Planned:
    fields = {k: v for k, v in ev.items() if k not in ("event_id", "event_type", "occurred_at", "source")}
    return Planned(json.dumps(ev).encode(), entity_key(ev["event_type"], fields), expect, ev["event_id"])


def build(scenario: str, tag: str, now: datetime | None = None, burst: int = 20) -> Manifest:
    now = now or datetime.now(timezone.utc)
    src = f"acceptance-{tag}"
    m = Manifest(tag)
    t = lambda minutes: now - timedelta(minutes=minutes)          # noqa: E731
    scen = ("normal", "duplicates", "out-of-order", "invalid") if scenario == "all" else (scenario,)
    for sc in scen:
        if sc == "normal":
            m.sends.append([
                _valid(make("inventory.adjusted", t(2), src, product_sku="P-1003", location="WEST-DC", delta=-2,
                            reason="cycle count correction")),
                _valid(make("stock.received", t(2), src, product_sku="P-1001", location="EAST-DC", quantity=12)),
                _valid(make("order.status_changed", t(2), src, order_id="SO-101996", customer_id=GLOBEX,
                            from_status="Processing", to_status="OnHold")),
                _valid(make("shipment.delayed", t(2), src, order_id="SO-101991", customer_id=INITECH,
                            expected_date=(now + timedelta(days=6)).date().isoformat(),
                            reason="carrier capacity")),
            ])
        elif sc == "duplicates":
            e1 = make("stock.received", t(3), src, product_sku="P-4005", location="EAST-DC", quantity=8)
            e2 = make("inventory.adjusted", t(3), src, product_sku="P-4004", location="EAST-DC", delta=3,
                      reason="returned to stock")
            m.sends.append([_valid(e1), _valid(e2)])
            m.sends.append([_valid(e1, "duplicate"), _valid(e2, "duplicate")])     # redelivery, separate batch
            m.sends.append([_valid(e1, "duplicate")])                               # and once more
        elif sc == "out-of-order":
            newer = make("order.status_changed", t(1), src, order_id="SO-101993", customer_id=ACME,
                         from_status="Processing", to_status="Shipped")
            older = make("order.status_changed", t(15), src, order_id="SO-101993", customer_id=ACME,
                         from_status="Processing", to_status="OnHold")
            m.sends.append([_valid(newer)])
            m.sends.append([_valid(older, "late")])                                 # arrives after the newer one
        elif sc == "invalid":
            bad = [
                b"{this is not json " + tag.encode() + b"}",
                json.dumps({"event_id": str(uuid.uuid4()), "event_type": "inventory.teleported",
                            "occurred_at": t(1).isoformat(), "source": src}).encode(),
                json.dumps(make("stock.received", now + timedelta(hours=2), src, product_sku="P-1001",
                                location="EAST-DC", quantity=1)).encode(),
                json.dumps(make("order.status_changed", t(1), src, order_id="DROP TABLE orders",
                                customer_id=GLOBEX, from_status="Processing", to_status="OnHold")).encode(),
                json.dumps({k: v for k, v in make("shipment.delayed", t(1), src, order_id="SO-101996",
                                                    customer_id=GLOBEX, expected_date="2026-11-30",
                                                    reason="x").items() if k != "occurred_at"}).encode(),
            ]
            m.sends.append([Planned(b, None, "rejected") for b in bad])
        elif sc == "burst":
            skus = ["P-1001", "P-1003", "P-4005", "P-4002", "P-1004"]
            rnd = random.Random(tag)        # locations unique to this run, so earlier history can't make events late
            batch = [_valid(make("inventory.adjusted", t(rnd.randint(0, 30)), src, product_sku=rnd.choice(skus),
                                 location=rnd.choice([f"BURST-{tag.upper()}-A", f"BURST-{tag.upper()}-B"])[:40],
                                 delta=rnd.choice([-3, -1, 2, 5]),
                                 reason="burst test")) for _ in range(burst)]
            # an event is 'late' if its entity already has a newer (occurred_at, event_id) - same rule as the
            # database (UUIDs compare like their lowercase hex text)
            latest: dict[str, tuple] = {}
            for p in batch:
                ev = json.loads(p.body)
                me = (datetime.fromisoformat(ev["occurred_at"]), ev["event_id"])
                k = p.partition_key
                if k in latest and latest[k] > me:
                    p.expect = "late"
                else:
                    latest[k] = me
            m.sends.append(batch)
        else:
            raise ValueError(f"unknown scenario {sc}")
    return m


def publish(manifest: Manifest, namespace: str | None = None, hub: str | None = None) -> int:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.identity import DefaultAzureCredential
    ns = namespace or os.environ["FR_EVENTHUB_NAMESPACE"]
    if "." not in ns:
        ns += ".servicebus.windows.net"
    producer = EventHubProducerClient(ns, hub or os.environ.get("FR_EVENTHUB_NAME", "fr-signals"),
                                      credential=DefaultAzureCredential())
    sent = 0
    with producer:
        for group in manifest.sends:
            by_key: dict[str | None, list[Planned]] = {}
            for p in group:
                by_key.setdefault(p.partition_key, []).append(p)
            for key, items in by_key.items():
                batch = producer.create_batch(partition_key=key) if key else producer.create_batch()
                for p in items:
                    batch.add(EventData(p.body))
                producer.send_batch(batch)
                sent += len(items)
    return sent
