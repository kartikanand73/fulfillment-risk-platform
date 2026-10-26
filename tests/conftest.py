"""PostgreSQL fixtures. Uses a real server (FR_TEST_ADMIN_DSN, a superuser connection); each test session gets
its own throw-away database. Integration tests are skipped - loudly - if no server is reachable."""
from __future__ import annotations

import os
import uuid

import pytest

ADMIN = os.environ.get("FR_TEST_ADMIN_DSN", "postgresql://postgres@/postgres?host=/tmp&port=55432")


TEST_PW = "fr_test_only"   # throw-away test roles on a throw-away server; never used outside tests


def _dsn(user: str, db: str) -> str:
    base = ADMIN.split("://", 1)[1]
    hostpart = base.split("@", 1)[1]            # host.../db?params
    _, _, rest = hostpart.partition("/")
    params = rest.partition("?")[2]
    host = hostpart.split("/", 1)[0]
    return f"postgresql://{user}@{host}/{db}" + (f"?{params}" if params else "")


@pytest.fixture(scope="session")
def pg():
    psycopg = pytest.importorskip("psycopg")
    try:
        admin = psycopg.connect(ADMIN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not reachable at FR_TEST_ADMIN_DSN ({e.__class__.__name__})")
    db = f"fr_test_{uuid.uuid4().hex[:8]}"
    admin.execute(f'CREATE DATABASE "{db}"')
    from fulfillment_risk.store import apply_schema
    owner = _dsn(ADMIN.split("://", 1)[1].split("@", 1)[0], db)
    apply_schema(owner)
    for login, group in (("fr_refresh", "fr_writer"), ("fr_agent", "fr_reader")):
        admin.execute(f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='{login}') "
                      f"THEN CREATE ROLE {login} LOGIN; END IF; END $$")
        admin.execute(f"ALTER ROLE {login} PASSWORD '{TEST_PW}'")
        admin.execute(f"GRANT {group} TO {login}")
    admin.execute(f'GRANT CONNECT ON DATABASE "{db}" TO fr_refresh, fr_agent')
    yield {"owner": owner, "writer": _dsn(f"fr_refresh:{TEST_PW}", db), "reader": _dsn(f"fr_agent:{TEST_PW}", db)}
    admin.execute(f'DROP DATABASE "{db}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(pg):
    """Empty fr tables for each test."""
    import psycopg
    with psycopg.connect(pg["owner"], autocommit=True) as c:
        c.execute("TRUNCATE fr.published_snapshot, fr.customer_risk, fr.order_line_risk, fr.sku_risk, "
                  "fr.refresh_run RESTART IDENTITY CASCADE")
    return pg
