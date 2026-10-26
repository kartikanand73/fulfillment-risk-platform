"""The Function handler with real azure.functions EventHubEvent objects against PostgreSQL."""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

func = pytest.importorskip("azure.functions")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "function_app"))
import function_app  # noqa: E402

from fulfillment_risk.signals import make  # noqa: E402

NOW = datetime.now(timezone.utc)


def event(body, seq, part="1"):
    from azure.functions.meta import Datum
    meta = {"PartitionContext": Datum(json.dumps({"PartitionId": part, "ConsumerGroup": "fr-signals-processor"}),
                                      "json")}                                   # what the host sends
    return func.EventHubEvent(body=body if isinstance(body, bytes) else json.dumps(body).encode(),
                              trigger_metadata=meta, sequence_number=seq,
                              enqueued_time=(NOW - timedelta(seconds=1)).replace(tzinfo=None))


def test_handle_batch_and_log_line(db, caplog):
    good = make("stock.received", NOW - timedelta(minutes=1), product_sku="P-1001", location="EAST-DC", quantity=4)
    caplog.set_level(logging.INFO, "fr_signals")
    res = function_app.handle([event(good, 10), event(good, 11), event(b"garbage", 12)],
                              connect=lambda: psycopg.connect(db["signals"]), now=NOW)
    assert (res.accepted, res.duplicates, res.rejected) == (1, 1, 1)
    assert any("fr_signals_batch accepted=1 duplicates=1" in r.message for r in caplog.records)
    assert any("fr_signals_rejected count=1" in r.message for r in caplog.records)
    with psycopg.connect(db["owner"]) as c:
        row = c.execute("SELECT partition_id, sequence_number, enqueued_at IS NOT NULL FROM fr.signal_event").fetchone()
        assert row == ("1", 10, True)


def test_database_error_raises_so_the_batch_is_retried(db):
    def broken():
        raise psycopg.OperationalError("server closed the connection")
    with pytest.raises(psycopg.OperationalError):
        function_app.handle([event(b"{}", 1)], connect=broken)


def test_function_is_registered_with_identity_connection():
    fns = function_app.app.get_functions()
    assert [f.get_function_name() for f in fns] == ["process_signals"]
    b = json.loads(fns[0].get_function_json())["bindings"][0]
    assert b["type"] == "eventHubTrigger" and b["connection"] == "EventHubConnection"
    assert b["cardinality"].lower() == "many" and b["consumerGroup"] == "%EVENTHUB_CONSUMER_GROUP%"
