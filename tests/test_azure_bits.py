"""Azure-only behaviour that can be checked offline: Entra token as the PostgreSQL password, telemetry no-op."""
import sys
import types

from fulfillment_risk.store import postgres


def test_entra_token_used_as_password(monkeypatch):
    seen = {}

    class Cred:
        def get_token(self, scope):
            seen["scope"] = scope
            return types.SimpleNamespace(token="ENTRA-TOKEN")

    monkeypatch.setitem(sys.modules, "azure.identity", types.SimpleNamespace(DefaultAzureCredential=Cred))
    monkeypatch.setattr(postgres.psycopg, "connect", lambda dsn, **kw: (dsn, kw))
    monkeypatch.setenv("FR_DATABASE_ENTRA", "true")
    dsn, kw = postgres.connect("host=x.postgres.database.azure.com dbname=fr user=id-fr-refresh")
    assert kw["password"] == "ENTRA-TOKEN" and kw["sslmode"] == "require"
    assert seen["scope"] == "https://ossrdbms-aad.database.windows.net/.default"
    monkeypatch.delenv("FR_DATABASE_ENTRA")
    assert "password" not in postgres.connect("host=localhost")[1]


def test_telemetry_is_a_noop_without_connection_string(monkeypatch):
    monkeypatch.delenv("APPLICATIONINSIGHTS_CONNECTION_STRING", raising=False)
    from fulfillment_risk.telemetry import flush, refresh_span
    with refresh_span(trigger="test") as span:
        span(status="succeeded", rows=3)
    flush()
