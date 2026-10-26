"""The real systems of record, configured from environment variables (see .env.example)."""
from __future__ import annotations

from .databricks import DatabricksConfig, DatabricksOpenOrders
from .salesforce import SalesforceConfig, SalesforceCustomers
from .snowflake import SnowflakeConfig, SnowflakeInventory


def live_sources():
    return (SalesforceCustomers(SalesforceConfig.from_env()),
            DatabricksOpenOrders(DatabricksConfig.from_env()),
            SnowflakeInventory(SnowflakeConfig.from_env()))
