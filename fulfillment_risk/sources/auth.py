"""Credentials for the live connectors. Nothing here logs or returns a secret anywhere but to the caller.

  secret_from(name, env_var)  Key Vault secret (FR_KEYVAULT_URL, read with DefaultAzureCredential:
                              managed identity when hosted, `az login` locally) or, for local dev only, an env var
  entra_token_provider(scope) bearer-token callable from DefaultAzureCredential (Databricks)
"""
from __future__ import annotations

import os


class CredentialError(Exception):
    pass


def secret_from(name: str, env_var: str):
    """Return a callable that fetches the secret on demand (so a rotated secret is picked up on the next run)."""
    def get() -> str:
        vault = os.environ.get("FR_KEYVAULT_URL", "").strip()
        if vault:
            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient
            except ImportError as e:  # pragma: no cover
                raise CredentialError("install the 'azure' extra to read Key Vault") from e
            return SecretClient(vault, DefaultAzureCredential()).get_secret(name).value
        v = os.environ.get(env_var, "")
        if not v:
            raise CredentialError(f"no Key Vault configured and {env_var} is not set")
        return v
    return get


def entra_token_provider(scope: str):
    from azure.identity import DefaultAzureCredential, get_bearer_token_provider
    return get_bearer_token_provider(DefaultAzureCredential(), scope)


def require_select(sql: str) -> str:
    """Version 1 is read-only: connectors may only send a single SELECT/WITH statement."""
    s = sql.strip().rstrip(";").strip()
    head = s.split(None, 1)[0].upper() if s else ""
    if head not in ("SELECT", "WITH") or ";" in s:
        raise ValueError("connectors may only run a single read-only SELECT statement")
    return s
