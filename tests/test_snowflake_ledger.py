"""Tests for idempotent Snowflake snapshot-ledger column evolution."""

from __future__ import annotations

import re

import pytest

from toronto_housing_ingestion.snowflake_ledger import (
    ensure_snapshot_ledger_quarantine_columns,
)


class FakeCursor:
    def __init__(self, columns=()):
        self.columns = {str(column).upper() for column in columns}
        self.statements = []
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            assert params is not None
            assert params[1] == "TORONTO_SNAPSHOT_LEDGER"
            self._rows = [(column,) for column in sorted(self.columns)]
        elif sql.startswith("ALTER TABLE"):
            match = re.search(r'ADD COLUMN "([A-Z0-9_]+)"', sql)
            assert match is not None
            self.columns.add(match.group(1))
            self._rows = []
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")
        return self

    def fetchall(self):
        return self._rows


def test_existing_quarantine_columns_are_not_altered_again() -> None:
    columns = {
        "QUARANTINED_ROW_COUNT",
        "QUARANTINED_BAD_ROW_COUNT",
        "QUARANTINED_BUSINESS_KEY_COUNT",
        "QUARANTINE_SHA256",
    }
    cursor = FakeCursor(columns)

    ensure_snapshot_ledger_quarantine_columns(cursor, "RAW_STAGE_123456789_1")

    assert len(cursor.statements) == 1
    assert "INFORMATION_SCHEMA.COLUMNS" in cursor.statements[0][0]


def test_missing_columns_are_added_with_defaults_and_without_if_not_exists() -> None:
    cursor = FakeCursor()

    ensure_snapshot_ledger_quarantine_columns(cursor, "RAW_STAGE_123456789_1")

    alters = [
        sql for sql, _params in cursor.statements if sql.startswith("ALTER TABLE")
    ]
    assert len(alters) == 4
    assert all("IF NOT EXISTS" not in sql for sql in alters)
    assert sum("DEFAULT 0" in sql for sql in alters) == 3
    assert any('ADD COLUMN "QUARANTINE_SHA256" VARCHAR' in sql for sql in alters)


def test_column_migration_is_idempotent() -> None:
    cursor = FakeCursor()

    ensure_snapshot_ledger_quarantine_columns(cursor, "RAW_STAGE_123456789_1")
    ensure_snapshot_ledger_quarantine_columns(cursor, "RAW_STAGE_123456789_1")

    alters = [
        sql for sql, _params in cursor.statements if sql.startswith("ALTER TABLE")
    ]
    assert len(alters) == 4


def test_unsafe_schema_name_is_rejected_before_query() -> None:
    cursor = FakeCursor()

    with pytest.raises(ValueError, match="Unsafe Snowflake ledger schema"):
        ensure_snapshot_ledger_quarantine_columns(
            cursor, 'RAW"; DROP SCHEMA TORONTO.RAW'
        )

    assert cursor.statements == []
