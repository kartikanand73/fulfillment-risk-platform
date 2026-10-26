# Fulfillment Risk Platform

> **Which customers in Salesforce have open orders in Databricks that cannot be fulfilled using the current inventory in Snowflake?**

This project answers that question with a deterministic refresh. It keeps only the answer, as a trusted snapshot in PostgreSQL, for an agent to explain in conversation.

**Governing principle:** *Preserve Salesforce, Databricks and Snowflake. Orchestrate only the fulfillment-risk outcome. Materialize the smallest trusted result.*

```
Salesforce (customers)   Databricks (open orders)   Snowflake (inventory)
          \                       |                        /
           └──── fulfillment-risk refresh: plain Python, zero model tokens ────┘
                                  |
              PostgreSQL fr schema: trusted snapshot + run history
                                  |
        Foundry agent: fulfillment-risk skill (conversational investigation)   [Phase 7]
```

## Status

| Phase | What | State |
|---|---|---|
| 1 | Data contract, read-only source interfaces, synthetic adapters | ✅ done |
| 2 | Deterministic allocation and classification | ✅ done |
| 3 | PostgreSQL run and snapshot tables, safe publish, roles, current views | ✅ done |
| 4 | Tests and failure handling (real PostgreSQL) | ✅ all passing |
| 5 | Real connectors: Salesforce SOQL, Databricks SQL (not Genie), Snowflake SQL API | ✅ verified live (dry run) |
| 6 | Scheduled Container Apps Job and Azure PostgreSQL Flexible Server | not started |
| 7 | `fulfillment-risk` skill on the Foundry hosted agent (Entra role `SkillAgent.FulfillmentRisk`) | not started |

This project has no Azure compute or database yet. Phase 5 reuses the existing Key Vault, Databricks warehouse and Snowflake account. Power BI is out of scope.

## Live sources (Phase 5)

| System | What the refresh reads | Identity | Setup |
|---|---|---|---|
| Salesforce | `SELECT Id, Name, NumberOfEmployees, Owner.Name FROM Account` (REST query API, GET only) | existing connected app (client credentials); secret in Key Vault `salesforce-client-secret` | none |
| Databricks | `skillagent.sales.v_open_order_lines` (orders with status Processing, joined to lines) through **Databricks SQL** on warehouse `skillagent-genie-wh` | Entra token (`az login` locally; the job's managed identity in Phase 6) | `deploy/databricks/open_orders_view.py` |
| Snowflake | `SUPPLY_CHAIN_DEMO.INVENTORY.INVENTORY_POSITION` through the SQL API | service user `FR_REFRESH_SVC` with role `FR_READER` (SELECT on that table only); PAT in Key Vault `snowflake-fr-pat`, 90 days | `deploy/snowflake/inventory_setup.py` |

Every statement passes through `require_select` (a single SELECT or WITH only), and every adapter exposes `fetch_*` methods only.

```bash
python deploy/local_env.py ../skill-agent/.env        # non-secret settings only
python -m fulfillment_risk check-sources --live       # row counts + freshness per source
python -m fulfillment_risk refresh --live --dry-run   # full gates + calculation, no database
```

**Live result, 2026-10-26 (dry run):** 14 open orders and 41 lines across 3 customers. 2 customers are at risk, with $40,253.40 short across 3 SKUs.

| Customer | Status | Short | Why |
|---|---|---|---|
| Globex Inc | UNFULFILLABLE | $20,253.40 | SO-101996: 23 Rugged Tablets (P-1003) with every unit reserved; 4 of 20 Protective Cases (P-4005) because Acme and Initech ordered earlier |
| Initech | PARTIAL | $20,000.00 | SO-101991: 22 of 38 Industrial Sensor Kits (P-1001); Acme's earlier order took 18 of 40 |
| Acme Corp | FULFILLABLE | $0 | |

## Quick start (local)

```bash
pip install -e ".[dev]"
# a PostgreSQL 16 superuser connection for tests (each run creates and drops its own database)
export FR_TEST_ADMIN_DSN=postgresql://postgres@localhost:5432/postgres
pytest -v

# try it against a database
createdb fr
FR_DATABASE_URL=postgresql://postgres@localhost/fr python -m fulfillment_risk init-db   # schema + roles (admin)
psql fr -c "CREATE ROLE fr_refresh LOGIN; GRANT fr_writer TO fr_refresh; CREATE ROLE fr_agent LOGIN; GRANT fr_reader TO fr_agent"
FR_DATABASE_URL=postgresql://fr_refresh@localhost/fr python -m fulfillment_risk refresh     # synthetic sources
FR_DATABASE_URL=postgresql://fr_agent@localhost/fr   python -m fulfillment_risk show --at-risk
FR_DATABASE_URL=postgresql://fr_agent@localhost/fr   python -m fulfillment_risk lines --order SO-9003
FR_DATABASE_URL=postgresql://fr_agent@localhost/fr   python -m fulfillment_risk status
```

On the synthetic dataset the answer is:

| Customer | Status | Short value | Why |
|---|---|---|---|
| Initech (`001fj000026m1nLAAQ`) | UNFULFILLABLE | $17,000.00 | SO-9003: Globex took the Edge Gateways first on the same day (Enterprise tier), leaving Initech 3 of 8. SO-9005: Industrial Sensor Kits are all reserved. |
| Acme Corp (`001fj000026lzjWAAQ`) | PARTIAL | $4,655.00 | SO-9004 gets 20 of 25 Rugged Tablets. |
| Globex Inc, Umbrella (synthetic) | FULFILLABLE | $0 | |

The total short value is $21,655.00 across 3 SKUs.

## How a refresh works

1. **Lock.** The refresh takes a PostgreSQL advisory lock. If another refresh is running, this one stops with `skipped`.
2. **Record the run.** It records a `running` row in `fr.refresh_run`. A run left `running` by a crashed process is marked `abandoned`.
3. **Fetch.** It reads the three sources. Each returns its rows plus an `as_of` time, which is recorded on the run.
4. **Gates.** Any failed gate means the run fails and nothing is published:
   - every source answered;
   - no source is older than `FR_MAX_SOURCE_AGE_HOURS` (24 by default);
   - customers and inventory are not empty;
   - data-issue lines are at most `FR_MAX_DATA_ISSUE_RATIO` (10% by default).
5. **Compute.** `engine.compute` allocates stock and classifies lines, orders and customers.
6. **Publish.** One transaction writes the results and moves `fr.published_snapshot` to this run. An error rolls everything back and marks the run `failed`, and **the previous snapshot stays published**.
7. **Retain.** It keeps the newest `FR_RETENTION_RUNS` runs (50 by default) and never deletes the published one.

## What readers see (the `fr_reader` role, views only)

| View | Grain | Includes |
|---|---|---|
| `fr.v_snapshot_status` | 1 row | run_id, `snapshot_at`, `salesforce_as_of` / `databricks_as_of` / `snowflake_as_of`, summary, failures since the snapshot |
| `fr.v_fulfillment_risk_current` | customer | status, open and at-risk orders, short units and value, blocked order value, worst order, plus snapshot time and freshness |
| `fr.v_fulfillment_risk_lines_current` | order line | allocated and short quantity, allocation rank, stock before the line, reason, plus snapshot time and freshness |
| `fr.v_sku_risk_current` | SKU | available, demanded, allocated, short |

Every row carries the snapshot time and each source's freshness, so any answer built on it can state both.

## Layout

```
fulfillment_risk/
  contracts.py        Customer, OpenOrderLine, InventoryPosition, SourceBatch (rows + as_of), Status
  sources/base.py     read-only interfaces (fetch_* only) + SourceError
  sources/synthetic.py  synthetic adapters with a known answer + Scenario switches for tests
  engine.py           allocation + classification (pure, deterministic)
  store/schema.sql    fr schema, views, roles, grants
  store/postgres.py   SnapshotStore (lock, run lifecycle, atomic publish, retention) + readers
  refresh.py          one refresh with gates; never calls a model
  cli.py              init-db | refresh | status | show | lines
tests/                engine, scenario, sources, refresh+store (real PostgreSQL), no-model checks
docs/                 architecture.md, data-contract.md, decisions.md
```

See [docs/decisions.md](docs/decisions.md) for the rules (allocation order, what counts as open, data issues, freshness).
