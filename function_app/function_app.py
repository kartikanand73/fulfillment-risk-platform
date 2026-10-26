"""Azure Function (Python v2): Event Hubs trigger -> fr.signal_* tables. Identity-based end to end:
  * Event Hubs: connection "EventHubConnection" = fullyQualifiedNamespace + managed identity (Data Receiver role)
  * PostgreSQL: Entra token of the same managed identity, PostgreSQL role fr_signal_writer
  * no connection strings, keys or passwords anywhere
At-least-once delivery + retries are safe because event_id is the idempotency key (see signals/processor.py).
"""
import logging
import os
import typing
from datetime import datetime, timezone

import azure.functions as func

from fulfillment_risk.signals import Message, process_batch

app = func.FunctionApp()
log = logging.getLogger("fr_signals")
OSSRDBMS_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"
_cred = None


def _connect():
    global _cred
    import psycopg
    from azure.identity import DefaultAzureCredential
    if _cred is None:
        _cred = DefaultAzureCredential(managed_identity_client_id=os.environ.get("AZURE_CLIENT_ID"))
    tok = _cred.get_token(OSSRDBMS_SCOPE).token
    return psycopg.connect(os.environ["FR_DATABASE_URL"], password=tok, sslmode="require", connect_timeout=15)


def _partition(ev) -> str | None:
    md = getattr(ev, "metadata", None) or {}
    pc = md.get("PartitionContext") or md.get("partitionContext") or {}
    return pc.get("PartitionId") or pc.get("partitionId")


def to_messages(events) -> list[Message]:
    out = []
    for ev in events:
        enq = getattr(ev, "enqueued_time", None)
        if enq is not None and enq.tzinfo is None:
            enq = enq.replace(tzinfo=timezone.utc)
        out.append(Message(ev.get_body(), enq, _partition(ev), getattr(ev, "sequence_number", None)))
    return out


def handle(events, connect=_connect, now: datetime | None = None):
    """Process one trigger batch. Raises on database errors so the Functions retry policy re-delivers the batch
    (nothing from a failed batch is committed)."""
    msgs = to_messages(events)
    with connect() as conn:
        res = process_batch(conn, msgs, now)
    log.info(res.log_line())
    if res.rejected:
        log.warning("fr_signals_rejected count=%d reasons=%s", res.rejected, res.reasons)
    return res


@app.function_name(name="process_signals")
@app.event_hub_message_trigger(arg_name="events", event_hub_name="%EVENTHUB_NAME%", connection="EventHubConnection",
                               consumer_group="%EVENTHUB_CONSUMER_GROUP%", cardinality="many",
                               data_type="binary")   # raw bytes: the HOST must not JSON-parse bodies - one malformed
                                                     # event would fail (and after retries, skip) the whole batch
@app.retry(strategy="exponential_backoff", max_retry_count="5", minimum_interval="00:00:02",
           maximum_interval="00:00:30")
def process_signals(events: typing.List[func.EventHubEvent]) -> None:   # real type: the worker inspects it
    handle(events)
