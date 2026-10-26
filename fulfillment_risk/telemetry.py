"""Optional Application Insights tracing for refresh runs (no-op unless APPLICATIONINSIGHTS_CONNECTION_STRING is set
and azure-monitor-opentelemetry is installed). Never records row contents - only counts, statuses and timings."""
from __future__ import annotations

import contextlib
import os

_tracer = None


def _init():
    global _tracer
    if _tracer is not None or not os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        return _tracer
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry import trace
    except ImportError:
        return None
    os.environ.setdefault("OTEL_SERVICE_NAME", "fulfillment-risk-refresh")
    configure_azure_monitor()
    _tracer = trace.get_tracer("fulfillment_risk")
    return _tracer


@contextlib.contextmanager
def refresh_span(**attrs):
    """Yields a setter for attributes; works (and does nothing) without App Insights."""
    t = _init()
    if t is None:
        yield lambda **kw: None
        return
    with t.start_as_current_span("fulfillment_risk.refresh") as sp:
        def set_(**kw):
            for k, v in kw.items():
                if v is not None:
                    sp.set_attribute(f"fr.{k}", v if isinstance(v, (int, float, bool)) else str(v))
        set_(**attrs)
        yield set_


def flush():
    if _tracer is not None:
        with contextlib.suppress(Exception):
            from opentelemetry import trace
            trace.get_tracer_provider().force_flush(10_000)
