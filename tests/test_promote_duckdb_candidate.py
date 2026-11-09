"""Tests for DuckDB candidate quarantine source-to-table resolution."""

from __future__ import annotations

from datetime import UTC, datetime

from scripts import promote_duckdb_candidate as promoter


class FakeResult:
    def __init__(self, *, rows=(), row=None):
        self._rows = list(rows)
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self):
        self.queries = []
        self.ledger_rows = [
            ("snapshot-1", "active_permits", 1, 1, 0, None, "succeeded"),
            ("snapshot-1", "cleared_permits", 0, 0, 0, None, "succeeded"),
        ]
        self.quarantine_rows = [
            (
                "quarantine-1",
                "snapshot-1",
                "active_permits",
                1,
                "invalid_business_key",
                None,
                "row-hash-1",
                datetime(2026, 11, 9, tzinfo=UTC),
                '{"PERMIT_NUM": null}',
                "parquet-sha-1",
                "toronto-raw-snapshot-bundle-1-1.zip",
            )
        ]

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        if "FROM raw.toronto_snapshot_ledger" in sql:
            return FakeResult(rows=self.ledger_rows)
        if "FROM raw.toronto_scd2_quarantine" in sql:
            return FakeResult(rows=self.quarantine_rows)
        if "FROM raw.raw_active_permits" in sql:
            return FakeResult(row=(0,))
        raise AssertionError(f"Unexpected DuckDB query: {sql}")


def test_quarantine_source_names_resolve_to_raw_tables() -> None:
    connection = FakeConnection()

    promoter._validate_quarantine_reconciliation(
        connection,
        baseline_counts={"active_permits": 100, "cleared_permits": 100},
        baseline_snapshot_ids={"snapshot-1"},
    )

    checked = [
        sql for sql, _params in connection.queries if "COUNT(*) FROM raw." in sql
    ]
    assert checked == [
        "SELECT COUNT(*) FROM raw.raw_active_permits "
        "WHERE row_hash = ? AND _dlt_valid_from = ?"
    ]
