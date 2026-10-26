"""Signal processing against real PostgreSQL: idempotency (incl. concurrent duplicates), out-of-order, rejected,
atomic batches, least privilege, and 'context only' (the snapshot is untouched)."""
import json
import threading
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from fulfillment_risk.refresh import run_refresh
from fulfillment_risk.signals import Message, make, process_batch
from fulfillment_risk.sources.synthetic import Scenario, synthetic_sources

NOW = datetime.now(timezone.utc)
GLOBEX = "001fj000026l3pEAAQ"


def msg(event, seq=1, enq=None, part="0"):
    body = event if isinstance(event, bytes) else json.dumps(event).encode()
    return Message(body, enq or NOW - timedelta(seconds=2), part, seq)


def ev(minutes_ago=5, **kw):
    base = dict(product_sku="P-1003", location="WEST-DC", delta=5, reason="cycle count")
    base.update(kw)
    return make("inventory.adjusted", NOW - timedelta(minutes=minutes_ago), **base)


def run(dsn, messages):
    with psycopg.connect(dsn) as c:
        return process_batch(c, messages, NOW)


def q(dsn, sql, args=()):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, args).fetchall()


def test_accept_duplicate_and_redelivery(db):
    e = ev()
    r1 = run(db["signals"], [msg(e), msg(e, seq=2)])                 # duplicate inside one batch
    r2 = run(db["signals"], [msg(e, seq=3)])                          # redelivered later (retry / replay)
    assert (r1.accepted, r1.duplicates, r2.accepted, r2.duplicates) == (1, 1, 0, 1)
    assert q(db["owner"], "SELECT count(*), max(duplicate_count) FROM fr.signal_event")[0] == (1, 2)
    assert r1.max_lag_ms >= 2000 and r1.event_types == {"inventory.adjusted": 1}


def test_concurrent_duplicates_store_one_row(db):
    e, errors = ev(), []

    def worker():
        try:
            run(db["signals"], [msg(e)])
        except Exception as ex:  # pragma: no cover
            errors.append(ex)
    ts = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    assert q(db["owner"], "SELECT count(*), max(duplicate_count) FROM fr.signal_event")[0] == (1, 7)


def test_out_of_order_is_flagged_and_does_not_replace_latest(db):
    newer, older = ev(minutes_ago=1, delta=7), ev(minutes_ago=10, delta=-3)
    r = run(db["signals"], [msg(newer, 1), msg(older, 2)])
    assert (r.accepted, r.late) == (2, 1)
    late = dict(q(db["owner"], "SELECT event_id::text, is_late FROM fr.signal_event"))
    assert late[newer["event_id"]] is False and late[older["event_id"]] is True
    state = q(db["owner"], "SELECT event_id::text FROM fr.signal_entity_state WHERE entity_key='sku:P-1003@WEST-DC'")
    assert state == [(newer["event_id"],)]
    newest = ev(minutes_ago=0, delta=1)                               # a genuinely newer one still advances
    assert run(db["signals"], [msg(newest)]).late == 0
    assert q(db["owner"], "SELECT event_id::text FROM fr.signal_entity_state")[0][0] == newest["event_id"]


def test_late_detection_is_per_entity(db):
    a = ev(minutes_ago=1, location="EAST-DC")
    b = ev(minutes_ago=30, location="WEST-DC")                        # older, but a different entity
    assert run(db["signals"], [msg(a), msg(b)]).late == 0


def test_invalid_rejected_with_reason_batch_continues(db):
    good = ev()
    r = run(db["signals"], [msg(b"{oops"), msg(dict(ev(), event_type="nope")), msg(good)])
    assert (r.accepted, r.rejected, r.reasons) == (1, 2, {"bad_json": 1, "unknown_type": 1})
    rows = q(db["owner"], "SELECT reason, raw, sequence_number FROM fr.signal_rejected ORDER BY id")
    assert rows[0][0] == "bad_json" and rows[0][1] == "{oops" and rows[1][0] == "unknown_type"
    assert "fr_signals_batch accepted=1" in r.log_line() and "rejected=2" in r.log_line()


def test_batch_is_atomic_on_database_error(db, monkeypatch):
    import fulfillment_risk.signals.processor as p
    good = ev()
    monkeypatch.setattr(p, "ADVANCE_ENTITY", "SELECT %s::text, %s::text, %s::text, %s::text, 1/0")  # fail mid-batch
    with pytest.raises(psycopg.errors.DivisionByZero):
        run(db["signals"], [msg(good)])
    assert q(db["owner"], "SELECT count(*) FROM fr.signal_event")[0][0] == 0     # retry will redo it cleanly


def test_signal_writer_cannot_touch_snapshot(db):
    for stmt in ("SELECT * FROM fr.customer_risk", "DELETE FROM fr.published_snapshot",
                 "INSERT INTO fr.refresh_run (status, code_version) VALUES ('succeeded','x')",
                 "DELETE FROM fr.signal_event"):
        with psycopg.connect(db["signals"]) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute(stmt)


def test_reader_sees_views_only(db):
    run(db["signals"], [msg(ev()), msg(b"bad")])
    with psycopg.connect(db["reader"]) as c:
        s = c.execute("SELECT accepted_1h, rejected_1h, by_type_24h FROM fr.v_signal_summary").fetchone()
        assert s[0] == 1 and s[1] == 1 and s[2] == {"inventory.adjusted": 1}
        assert c.execute("SELECT count(*) FROM fr.v_signal_rejected_recent").fetchone()[0] == 1
    with psycopg.connect(db["reader"]) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute("SELECT * FROM fr.signal_event")


def test_signals_are_context_only(db):
    """Events join to the current snapshot for display but never change it."""
    out = run_refresh(db["writer"], *synthetic_sources(Scenario()), trigger="test")
    before = q(db["owner"], "SELECT customer_id, status, short_value FROM fr.customer_risk ORDER BY 1")
    on_hold = make("order.status_changed", NOW - timedelta(minutes=1), order_id="SO-9003",
                   customer_id="001fj000026m1nLAAQ", from_status="Processing", to_status="Cancelled")
    restock = make("stock.received", NOW - timedelta(minutes=1), product_sku="P-1002", location="EAST-DC", quantity=500)
    run(db["signals"], [msg(on_hold), msg(restock)])
    assert q(db["owner"], "SELECT customer_id, status, short_value FROM fr.customer_risk ORDER BY 1") == before
    assert q(db["owner"], "SELECT run_id FROM fr.published_snapshot")[0][0] == out.run_id
    with psycopg.connect(db["reader"]) as c:
        rows = {r[0]: r[1:] for r in c.execute(
            "SELECT event_type, customer_name, snapshot_order_status, snapshot_customer_status, "
            "snapshot_sku_short_units FROM fr.v_signal_recent").fetchall()}
    assert rows["order.status_changed"][:3] == ("Initech", "PARTIAL", "UNFULFILLABLE")
    assert rows["stock.received"][3] == 5                              # P-1002 is still short in the snapshot


def test_publisher_scenarios_match_pipeline(db):
    """Every planned send of the 'all' and 'burst' scenarios, pushed through the processor batch by batch,
    produces exactly the counts the manifest predicts (what the live acceptance test checks in Azure)."""
    from fulfillment_risk.signals.publisher import build
    for scenario in ("all", "burst"):
        m = build(scenario, f"t-{scenario}", now=NOW)
        got = {"accepted": 0, "duplicate": 0, "late": 0, "rejected": 0}
        seq = 0
        for group in m.sends:
            msgs = []
            for p in group:
                seq += 1
                msgs.append(msg(p.body, seq))
            r = run(db["signals"], msgs)
            got["accepted"] += r.accepted
            got["duplicate"] += r.duplicates
            got["late"] += r.late
            got["rejected"] += r.rejected
        assert got == m.expected(), scenario
