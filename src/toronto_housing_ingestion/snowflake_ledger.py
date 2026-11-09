"""Idempotent schema evolution for the Snowflake snapshot ledger."""

from __future__ import annotations

import re
from typing import Any

_QUARANTINE_COLUMNS = (
    ("QUARANTINED_ROW_COUNT", "NUMBER(38,0) DEFAULT 0"),
    ("QUARANTINED_BAD_ROW_COUNT", "NUMBER(38,0) DEFAULT 0"),
    ("QUARANTINED_BUSINESS_KEY_COUNT", "NUMBER(38,0) DEFAULT 0"),
    ("QUARANTINE_SHA256", "VARCHAR"),
)


def ensure_snapshot_ledger_quarantine_columns(cursor: Any, schema_name: str) -> None:
    """Add missing quarantine metadata columns without IF NOT EXISTS + DEFAULT.

    Snowflake does not allow IF NOT EXISTS together with DEFAULT in ADD COLUMN.
    Inspecting INFORMATION_SCHEMA first keeps this operation repeatable and
    preserves DEFAULT 0 for the three numeric counters when they are first added.
    """
    schema = str(schema_name).upper()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", schema):
        raise ValueError(f"Unsafe Snowflake ledger schema name: {schema_name!r}")

    cursor.execute(
        "SELECT COLUMN_NAME FROM TORONTO.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, "TORONTO_SNAPSHOT_LEDGER"),
    )
    existing = {str(row[0]).upper() for row in cursor.fetchall()}

    for column_name, definition in _QUARANTINE_COLUMNS:
        if column_name in existing:
            continue
        cursor.execute(
            f'ALTER TABLE TORONTO."{schema}".TORONTO_SNAPSHOT_LEDGER '
            f'ADD COLUMN "{column_name}" {definition}'
        )
        existing.add(column_name)
