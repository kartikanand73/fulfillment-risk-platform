"""The headline question on the synthetic dataset: which Salesforce customers have open Databricks orders
that current Snowflake inventory cannot fulfil? The answer is known in advance (synthetic.EXPECTED)."""
from fulfillment_risk.contracts import Status
from fulfillment_risk.engine import compute
from fulfillment_risk.sources.synthetic import (ACME, EXPECTED, GLOBEX, HOOLI, INITECH, Scenario,
                                                synthetic_sources)


def run(sc=None):
    sf, db, sn = synthetic_sources(sc)
    return compute(sf.fetch_customers().rows, db.fetch_open_order_lines().rows, sn.fetch_inventory().rows)


def test_expected_answer():
    r = run()
    status = {c.customer_id: c.status.value for c in r.customers}
    for cid, s in {**EXPECTED["customers_at_risk"], **EXPECTED["customers_ok"]}.items():
        assert status[cid] == s
    assert HOOLI not in status                                   # no open orders -> not in the result
    assert r.summary["short_value"] == EXPECTED["short_value_usd"]
    assert r.summary["customers_at_risk"] == 2 and r.summary["data_issue_lines"] == 0


def test_why_initech_is_short_on_edge_gateway():
    lines = {(l.order_id, l.line_number): l for l in run().lines}
    globex, initech = lines[("SO-9002", 1)], lines[("SO-9003", 1)]
    assert globex.allocation_rank < initech.allocation_rank          # same day, Enterprise first
    assert (initech.allocated_qty, initech.short_qty, initech.sku_available_before) == (3, 5, 3)
    assert initech.status == Status.PARTIAL


def test_customer_ranking_worst_first():
    assert [c.customer_id for c in run().customers][:2] == [INITECH, ACME]
    assert run().customers[-1].customer_id in (GLOBEX, "001SYNTHUMBRELLAAA")


def test_shuffled_sources_same_answer():
    assert run() == run(Scenario(shuffle=True))
