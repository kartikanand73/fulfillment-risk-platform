"""Operational signals: Event Hubs events shown as live context next to the fulfillment-risk snapshot.
They never change the snapshot; the next hourly refresh from the systems of record stays authoritative."""
from .contract import EVENT_TYPES, Invalid, Signal, make, parse
from .processor import BatchResult, Message, process_batch

__all__ = ["EVENT_TYPES", "Invalid", "Signal", "make", "parse", "BatchResult", "Message", "process_batch"]
