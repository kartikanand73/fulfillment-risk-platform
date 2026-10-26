"""Store a batch of Event Hubs messages: validate -> idempotent insert -> out-of-order check -> rejected table.

Guarantees (tested against real PostgreSQL):
  * idempotent      event_id is the primary key; a redelivery bumps duplicate_count instead of a second row
                    (safe under Event Hubs at-least-once delivery and Function retries)
  * out-of-order    per entity (order, or SKU@location) the newest (occurred_at, event_id) wins; an older event that
                    arrives later is stored with is_late = true and does not replace the entity's latest state
  * never blocks    an invalid message is recorded in fr.signal_rejected with a reason; the rest of the batch goes on
  * atomic batch    one transaction; on a database error nothing from the batch is kept and the Function retries
  * context only    touches fr.signal_* tables only - never the fulfillment-risk snapshot
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .contract import Invalid, Signal, parse

RAW_LIMIT = 4096


@dataclass
class Message:
    """What the Function hands over per Event Hubs event (decoupled from azure-functions types for tests)."""
    body: bytes
    enqueued_at: datetime | None = None
    partition_id: str | None = None
    sequence_number: int | None = None


@dataclass
class BatchResult:
    accepted: int = 0
    duplicates: int = 0
    late: int = 0
    rejected: int = 0
    reasons: dict = field(default_factory=dict)
    max_lag_ms: int = 0
    event_types: dict = field(default_factory=dict)

    def log_line(self) -> str:
        """Single structured line - the alert rules parse these fields from App Insights traces."""
        return (f"fr_signals_batch accepted={self.accepted} duplicates={self.duplicates} late={self.late} "
                f"rejected={self.rejected} max_lag_ms={self.max_lag_ms} "
                f"reasons={json.dumps(self.reasons, sort_keys=True)} types={json.dumps(self.event_types, sort_keys=True)}")


INSERT_EVENT = """
INSERT INTO fr.signal_event (event_id, event_type, entity_key, customer_id, order_id, product_sku, location, source,
                             occurred_at, enqueued_at, partition_id, sequence_number, payload)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
ON CONFLICT (event_id) DO UPDATE SET duplicate_count = fr.signal_event.duplicate_count + 1,
                                     last_duplicate_at = now()
RETURNING (xmax = 0) AS inserted"""

ADVANCE_ENTITY = """
INSERT INTO fr.signal_entity_state AS s (entity_key, event_id, event_type, occurred_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (entity_key) DO UPDATE SET event_id = EXCLUDED.event_id, event_type = EXCLUDED.event_type,
                                       occurred_at = EXCLUDED.occurred_at, updated_at = now()
WHERE (s.occurred_at, s.event_id) < (EXCLUDED.occurred_at, EXCLUDED.event_id)
RETURNING 1"""

INSERT_REJECTED = """
INSERT INTO fr.signal_rejected (event_id, reason, detail, raw, partition_id, sequence_number, enqueued_at)
VALUES (%s,%s,%s,%s,%s,%s,%s)"""


def _raw(body) -> str:
    if isinstance(body, (bytes, bytearray)):
        return body[:RAW_LIMIT].decode("utf-8", errors="replace")
    return str(body)[:RAW_LIMIT]


def process_batch(conn, messages: list[Message], now: datetime | None = None) -> BatchResult:
    """`conn` is a psycopg connection (not autocommit). Commits once for the whole batch."""
    now = now or datetime.now(timezone.utc)
    res = BatchResult()
    with conn.transaction():
        cur = conn.cursor()
        for m in messages:
            try:
                s: Signal = parse(m.body, now)
            except Invalid as e:
                cur.execute(INSERT_REJECTED, (e.event_id, e.reason, e.detail[:500], _raw(m.body), m.partition_id,
                                              m.sequence_number, m.enqueued_at))
                res.rejected += 1
                res.reasons[e.reason] = res.reasons.get(e.reason, 0) + 1
                continue
            cur.execute(INSERT_EVENT, (s.event_id, s.event_type, s.entity_key, s.customer_id, s.order_id,
                                       s.product_sku, s.location, s.source, s.occurred_at, m.enqueued_at,
                                       m.partition_id, m.sequence_number, json.dumps(s.payload, default=str)))
            if not cur.fetchone()[0]:
                res.duplicates += 1
                continue
            res.accepted += 1
            res.event_types[s.event_type] = res.event_types.get(s.event_type, 0) + 1
            if m.enqueued_at:
                res.max_lag_ms = max(res.max_lag_ms, int((now - m.enqueued_at).total_seconds() * 1000))
            cur.execute(ADVANCE_ENTITY, (s.entity_key, s.event_id, s.event_type, s.occurred_at))
            if cur.fetchone() is None:                      # an equal-or-newer event for this entity already arrived
                cur.execute("UPDATE fr.signal_event SET is_late = true WHERE event_id = %s", (s.event_id,))
                res.late += 1
    return res
