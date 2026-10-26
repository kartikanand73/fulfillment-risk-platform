# Data contract

## Join keys

| Key | Meaning | From → to |
|---|---|---|
| `customer_id` | Salesforce Account Id (18 characters), e.g. `001fj000026lzjWAAQ` | Salesforce `Account.Id` → Databricks `orders.salesforce_account_id` |
| `product_sku` | Product id used on Databricks order lines, e.g. `P-1002` | Databricks `order_lines.product_id` → Snowflake `INVENTORY.PRODUCT_SKU` |

## Records each source must return (`fulfillment_risk/contracts.py`)

Every source returns a `SourceBatch(source, as_of, rows)`. `as_of` is timezone-aware: the time the source says its data was current, not the time we fetched it.

| Source | Record | Fields | Phase 5 query |
|---|---|---|---|
| Salesforce | `Customer` | customer_id, name, tier, owner | SOQL: `SELECT Id, Name, <tier field>, Owner.Name FROM Account` |
| Databricks | `OpenOrderLine` | order_id, line_number, customer_id, product_sku, quantity, unit_net_value, order_date, status | Databricks SQL on a read-only view joining `orders` (status = Processing) and `order_lines`; unit_net_value = net_amount / quantity |
| Snowflake | `InventoryPosition` | product_sku, location, on_hand, reserved | Snowflake SQL API: `SELECT PRODUCT_SKU, LOCATION, ON_HAND, RESERVED, UPDATED_AT FROM INVENTORY` |

**Rules:**
- Quantities are positive integers.
- `(order_id, line_number)` is unique.
- Money is USD.
- An empty customer or inventory batch is treated as a broken extract.
- An empty order batch is valid: it means no open orders.

## Synthetic dataset (Phases 1–4)

The synthetic dataset uses the real Salesforce account ids and real Databricks SKUs, plus two synthetic customers:
- Umbrella: SMB tier, has orders.
- Hooli: has no open orders, so it must not appear in the result.

The expected answer is in `sources/synthetic.py` (`EXPECTED`) and is asserted in `tests/test_scenario.py`.

**Scenario switches** for tests:
- `fail={source}`: a source is down.
- `age={source: timedelta}`: stale data.
- `extra_lines`: unknown SKUs or customers.
- `inventory_override`, `customers_override`, `lines_override`: empty or alternative feeds.
- `shuffle`: rows arrive in a different order.
