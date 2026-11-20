"""Fail-closed authorization guard for Snowflake operations."""

from __future__ import annotations

import os


def require_snowflake_operations_enabled(operation: str) -> None:
    """Allow Snowflake operations only when the gate is exactly 'true'."""
    if os.environ.get("SNOWFLAKE_OPERATIONS_ENABLED") != "true":
        raise PermissionError(
            f"{operation} is disabled; "
            "SNOWFLAKE_OPERATIONS_ENABLED must be exactly 'true'."
        )
