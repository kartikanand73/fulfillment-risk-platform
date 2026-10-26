"""The scheduled refresh path must consume zero model tokens: it may not import or call any model SDK."""
import ast
import pathlib
import socket
import subprocess
import sys

PKG = pathlib.Path(__file__).resolve().parents[1] / "fulfillment_risk"
MODEL_SDKS = {"openai", "anthropic", "azure.ai", "langchain", "semantic_kernel", "agent_framework"}


def test_no_model_sdk_imported_anywhere_in_refresh_package():
    for f in PKG.rglob("*.py"):
        for node in ast.walk(ast.parse(f.read_text())):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                    [node.module or ""] if isinstance(node, ast.ImportFrom) else []
            for n in names:
                assert not any(n == m or n.startswith(m + ".") for m in MODEL_SDKS), f"{f.name} imports {n}"


def test_engine_runs_with_network_disabled(monkeypatch):
    def no_network(*a, **k):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket.socket, "connect", no_network)
    from fulfillment_risk.engine import compute
    from fulfillment_risk.sources.synthetic import synthetic_sources
    sf, db, sn = synthetic_sources()
    assert compute(sf.fetch_customers().rows, db.fetch_open_order_lines().rows, sn.fetch_inventory().rows).customers


def test_refresh_process_never_loads_a_model_sdk():
    code = ("import sys, fulfillment_risk.refresh, fulfillment_risk.cli, fulfillment_risk.sources.synthetic;"
            "bad=[m for m in sys.modules if m.split('.')[0] in ('openai','anthropic','langchain')];"
            "print(bad); sys.exit(1 if bad else 0)")
    r = subprocess.run([sys.executable, "-c", code], cwd=PKG.parent, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
