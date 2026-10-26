# Operational signals (Event Hubs → Function → PostgreSQL)

Events show up on the dashboard within seconds as **context**. They never change the deterministic fulfillment-risk calculation: the next hourly refresh from Salesforce, Databricks and Snowflake stays the only source of the numbers. The dashboard says so next to the signals, and a live acceptance check proves the snapshot is unchanged.

```
publisher (your Entra sign-in; role: Azure Event Hubs Data Sender on the hub)
  → Event Hubs evhns-fr-ka2026 (Standard, 1 TU, local auth DISABLED) / fr-signals (2 partitions, 24 h)
      consumer group fr-signals-processor   ($Default left free for future consumers)
  → Azure Function func-fr-signals-ka2026 (Flex Consumption, Python 3.11)
      trigger: identity connection (Data Receiver on the hub), cardinality many, dataType binary, retry x5 exp.
      validate → idempotent insert (event_id) → out-of-order check per entity → rejected table
  → PostgreSQL fr.signal_event | fr.signal_rejected | fr.signal_entity_state   (role fr_signal_writer)
  → views fr.v_signal_recent | v_signal_summary | v_signal_rejected_recent       (role fr_reader)
  → web app: "⚡ Live operational signals" section, reloads every 15 s
```

## Event contract v1 (`fulfillment_risk/signals/contract.py`)

Envelope: `event_id` (UUID), `event_type`, `occurred_at` (ISO 8601 with offset), `source`, plus:

| event_type | required | entity key (= partition key) |
|---|---|---|
| `inventory.adjusted` | product_sku, location, delta (≠0), reason | `sku:<sku>@<location>` |
| `stock.received` | product_sku, location, quantity (>0) | `sku:<sku>@<location>` |
| `order.status_changed` | order_id, customer_id, from_status, to_status | `order:<id>` |
| `shipment.delayed` | order_id, customer_id, expected_date, reason | `order:<id>` |

## Handling

| Case | Behaviour |
|---|---|
| Valid | Stored once, with partition, sequence number, enqueue time, receive time, and lag. |
| **Duplicate** (same `event_id`) | Not stored again. `duplicate_count` goes up and `last_duplicate_at` is set. This holds for redeliveries, Function retries and concurrent deliveries (tested with 8 parallel writers). |
| **Out of order** | Per entity, the newest `(occurred_at, event_id)` wins. An older event that arrives later is stored with `is_late = true` and doesn't replace the entity's latest state. |
| **Invalid** | One row in `fr.signal_rejected` with reason and detail, plus the first 4 KB of the body. Reasons: `bad_json`, `not_object`, `too_large`, `missing_field`, `bad_event_id`, `unknown_type`, `bad_timestamp`, `future_event` (more than 5 min ahead), `stale_event` (more than 7 days old), `bad_field`, `bad_source`. The rest of the batch carries on. |
| Database error | The whole batch rolls back and the Function raises, so the retry policy redelivers it. Idempotency makes the redelivery safe. |

## Telemetry and alerts (portal only; no action group, no email)

Each batch writes one line to App Insights:
`fr_signals_batch accepted= duplicates= late= rejected= max_lag_ms= reasons={} types={}`

| Alert | Rule |
|---|---|
| `fr-signals-rejected` (sev 2) | sum(rejected) > 5 in 15 min, evaluated every 5 min |
| `fr-signals-function-failures` (sev 1) | any failed `process_signals` invocation in 5 min |
| `fr-signals-no-activity` (sev 3, **created disabled**) | no batches for 2 h. Enable it once a real producer exists. |

## Run it

```bash
python deploy/azure/signals.py up              # everything (rerun-safe)
python deploy/azure/signals.py deploy          # code only (zip with 0644 files -> remote build)
python deploy/azure/signals.py smoke           # publish 4 events and read them back
python -m fulfillment_risk.signals publish --scenario all|normal|duplicates|out-of-order|invalid|burst
python deploy/azure/acceptance_signals.py      # live acceptance A1-A6
python deploy/azure/appi_query.py deploy/azure/queries/func_batches.kql
python deploy/azure/signals.py down [--drop-tables]   # teardown
```

## Lessons from the live acceptance run

1. **The host JSON-parsed bodies before the Python code saw them.** One malformed event failed the whole batch, which was retried 5 times and then skipped, losing 3 valid events with it. Fix: `data_type="binary"` on the trigger. A test now locks this in.
2. **First start with `initialOffsetOptions = fromEnd`** skipped events already in the hub. Fix: `fromStart`; replay is safe because storage is idempotent.
3. **Zip entries need Unix permissions** (0644), or the Linux worker can't read the package.
4. **Binding annotations must be real types** (`typing.List[func.EventHubEvent]`). `from __future__ import annotations` breaks function indexing.
