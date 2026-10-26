"""Read-only source interfaces. Version 1 never writes to a system of record, so these
protocols expose fetch methods only - there is nothing to call that could change data."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..contracts import Customer, InventoryPosition, OpenOrderLine, SourceBatch


class SourceError(Exception):
    """A source could not be read. The refresh fails and the last published snapshot stays."""

    def __init__(self, source: str, message: str):
        super().__init__(f"{source}: {message}")
        self.source = source


@runtime_checkable
class CustomerSource(Protocol):
    name: str

    def fetch_customers(self) -> SourceBatch[Customer]: ...


@runtime_checkable
class OpenOrderSource(Protocol):
    name: str

    def fetch_open_order_lines(self) -> SourceBatch[OpenOrderLine]: ...


@runtime_checkable
class InventorySource(Protocol):
    name: str

    def fetch_inventory(self) -> SourceBatch[InventoryPosition]: ...
