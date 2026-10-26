"""Source contracts: read-only, typed, timestamped."""
import inspect
from datetime import datetime

import pytest

from fulfillment_risk.contracts import SourceBatch
from fulfillment_risk.sources import CustomerSource, InventorySource, OpenOrderSource, SourceError
from fulfillment_risk.sources.synthetic import Scenario, synthetic_sources

WRITE_WORDS = ("write", "update", "insert", "delete", "upsert", "create", "put", "post", "patch", "save", "set_")


def test_synthetic_adapters_satisfy_protocols():
    sf, db, sn = synthetic_sources()
    assert isinstance(sf, CustomerSource) and isinstance(db, OpenOrderSource) and isinstance(sn, InventorySource)


@pytest.mark.parametrize("proto", [CustomerSource, OpenOrderSource, InventorySource])
def test_interfaces_are_read_only(proto):
    methods = [n for n, _ in inspect.getmembers(proto, inspect.isfunction) if not n.startswith("_")]
    assert methods and all(m.startswith("fetch_") for m in methods)
    assert not any(w in m for m in methods for w in WRITE_WORDS)


def test_adapters_expose_no_write_methods():
    for a in synthetic_sources():
        public = [n for n in dir(a) if not n.startswith("_") and callable(getattr(a, n))]
        assert all(n.startswith("fetch_") for n in public), public


def test_batches_are_timestamped():
    for b in (s() for s in (lambda: synthetic_sources()[0].fetch_customers(),
                            lambda: synthetic_sources()[1].fetch_open_order_lines(),
                            lambda: synthetic_sources()[2].fetch_inventory())):
        assert b.as_of.tzinfo is not None and b.row_count > 0
    with pytest.raises(ValueError, match="timezone-aware"):
        SourceBatch("x", datetime(2026, 1, 1))


def test_simulated_outage_raises_source_error():
    with pytest.raises(SourceError, match="snowflake"):
        synthetic_sources(Scenario(fail={"snowflake"}))[2].fetch_inventory()
