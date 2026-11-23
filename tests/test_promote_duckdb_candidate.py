"""Tests for DuckDB candidate quarantine source-to-table resolution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("status", ["succeeded", "excluded"])
def test_candidate_ledger_accepts_terminal_statuses(monkeypatch, status) -> None:
    class CandidateConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            if "information_schema.tables" in sql:
                names = set(promoter.TABLES) | {
                    "toronto_snapshot_ledger",
                    "toronto_scd2_quarantine",
                }
                return FakeResult(rows=[(name,) for name in names])

            if "FROM raw.toronto_snapshot_ledger" in sql:
                assert "NOT IN ('succeeded', 'excluded')" in sql
                return FakeResult(row=(1, 0 if status in {
                    "succeeded", "excluded"
                } else 1))

            return FakeResult(row=(0,))

    monkeypatch.setattr(
        promoter.duckdb, "connect", lambda *args, **kwargs: CandidateConnection()
    )
    monkeypatch.setattr(
        promoter,
        "_validate_quarantine_reconciliation",
        lambda *args, **kwargs: None,
    )

    promoter.validate_candidate(
        Path("candidate.duckdb"),
        baseline_counts={"active_permits": 100, "cleared_permits": 100},
        baseline_snapshot_ids={"snapshot-1"},
    )


@pytest.mark.parametrize(
    "status",
    ["pending", "loading", "failed", None, "unknown"],
)
def test_candidate_ledger_rejects_nonterminal_statuses(monkeypatch, status) -> None:
    class CandidateConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            if "information_schema.tables" in sql:
                names = set(promoter.TABLES) | {
                    "toronto_snapshot_ledger",
                    "toronto_scd2_quarantine",
                }
                return FakeResult(rows=[(name,) for name in names])

            if "FROM raw.toronto_snapshot_ledger" in sql:
                assert "NOT IN ('succeeded', 'excluded')" in sql
                return FakeResult(row=(1, 1))

            return FakeResult(row=(0,))

    monkeypatch.setattr(
        promoter.duckdb, "connect", lambda *args, **kwargs: CandidateConnection()
    )

    with pytest.raises(RuntimeError, match="empty or incomplete snapshot ledger"):
        promoter.validate_candidate(
            Path("candidate.duckdb"),
            baseline_counts={"active_permits": 100, "cleared_permits": 100},
            baseline_snapshot_ids={"snapshot-1"},
        )
