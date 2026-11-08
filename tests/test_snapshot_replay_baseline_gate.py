"""Fail-closed guards for canonical snapshot backlog replay."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import ModuleType

import duckdb
import pytest

if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        snowflake_package = ModuleType("snowflake")
        snowflake_package.__path__ = []
        connector_module = ModuleType("snowflake.connector")

        def _no_live_connection(**kwargs):
            raise AssertionError("Offline baseline tests must not connect to Snowflake")

        connector_module.connect = _no_live_connection
        snowflake_package.connector = connector_module
        sys.modules["snowflake"] = snowflake_package
        sys.modules["snowflake.connector"] = connector_module

from scripts import load_duckdb_snapshot as duck_loader
from scripts import snowflake_refresh_raw as snowflake_loader
from toronto_housing_ingestion import ingestion


def _set_contract(monkeypatch, *, enabled="true", baseline_id="123:1", count="1"):
    monkeypatch.setenv("TORONTO_CANONICAL_REPLAY_ENABLED", enabled)
    monkeypatch.setenv("TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID", baseline_id)
    monkeypatch.setenv("TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT", count)


def _create_ledger(database, rows=()):
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            "CREATE TABLE raw.toronto_snapshot_ledger "
            "(snapshot_id VARCHAR, source_run_id VARCHAR, source_run_attempt INTEGER, "
            "extraction_started_at_utc TIMESTAMPTZ, source_name VARCHAR, status VARCHAR)"
        )
        for row in rows:
            connection.execute(
                "INSERT INTO raw.toronto_snapshot_ledger VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )


def test_canonical_duckdb_replay_requires_explicit_enablement(tmp_path, monkeypatch):
    database = tmp_path / "canonical.duckdb"
    with duckdb.connect(str(database)):
        pass
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    monkeypatch.delenv("TORONTO_CANONICAL_REPLAY_ENABLED", raising=False)
    with pytest.raises(
        RuntimeError, match="disabled until the isolated historical rebuild"
    ):
        duck_loader._assert_canonical_replay_baseline(database)


def test_canonical_duckdb_replay_requires_promoted_baseline_id(tmp_path, monkeypatch):
    database = tmp_path / "canonical.duckdb"
    _create_ledger(database)
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    _set_contract(monkeypatch, baseline_id="", count="1")
    with pytest.raises(RuntimeError, match="must be run_id:attempt"):
        duck_loader._assert_canonical_replay_baseline(database)


def test_canonical_duckdb_replay_rejects_missing_baseline_snapshot(
    tmp_path, monkeypatch
):
    database = tmp_path / "canonical.duckdb"
    _create_ledger(database)
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    _set_contract(monkeypatch)
    with pytest.raises(RuntimeError, match="absent from the DuckDB ledger"):
        duck_loader._assert_canonical_replay_baseline(database)


def test_canonical_duckdb_replay_rejects_count_mismatch(tmp_path, monkeypatch):
    database = tmp_path / "canonical.duckdb"
    rows = [
        ("123:1", "123", 1, "2026-10-27T01:00:00+00:00", source, "succeeded")
        for source in ("active_permits", "cleared_permits")
    ]
    _create_ledger(database, rows)
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    _set_contract(monkeypatch, count="28")
    with pytest.raises(RuntimeError, match="expected 28 complete snapshots"):
        duck_loader._assert_canonical_replay_baseline(database)


def test_canonical_duckdb_replay_accepts_exact_complete_baseline(tmp_path, monkeypatch):
    database = tmp_path / "canonical.duckdb"
    rows = [
        ("123:1", "123", 1, "2026-10-27T01:00:00+00:00", source, "succeeded")
        for source in ("active_permits", "cleared_permits")
    ]
    _create_ledger(database, rows)
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    _set_contract(monkeypatch, count="1")
    duck_loader._assert_canonical_replay_baseline(database)


def test_canonical_duckdb_replay_rejects_incomplete_source(tmp_path, monkeypatch):
    database = tmp_path / "canonical.duckdb"
    rows = [
        ("123:1", "123", 1, "2026-10-27T01:00:00+00:00", "active_permits", "succeeded"),
        ("123:1", "123", 1, "2026-10-27T01:00:00+00:00", "cleared_permits", "failed"),
    ]
    _create_ledger(database, rows)
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(database))
    _set_contract(monkeypatch, count="1")
    with pytest.raises(
        RuntimeError, match="found 1 ledger snapshots and 0 fully completed"
    ):
        duck_loader._assert_canonical_replay_baseline(database)


def test_isolated_duckdb_file_does_not_need_canonical_promotion(monkeypatch, tmp_path):
    canonical = tmp_path / "canonical.duckdb"
    isolated = tmp_path / "history-validation.duckdb"
    monkeypatch.setenv("TORONTO_CANONICAL_DUCKDB_PATH", str(canonical))
    monkeypatch.delenv("TORONTO_CANONICAL_REPLAY_ENABLED", raising=False)
    duck_loader._assert_canonical_replay_baseline(isolated)


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query_count = 0

    def execute(self, query, params=None):
        self.query_count += 1

    def fetchone(self):
        return (1,)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, rows):
        self._cursor = _FakeCursor(rows)

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def test_snowflake_canonical_replay_requires_explicit_enablement(monkeypatch):
    monkeypatch.delenv("TORONTO_CANONICAL_REPLAY_ENABLED", raising=False)
    with pytest.raises(
        RuntimeError, match="disabled until the isolated historical rebuild"
    ):
        snowflake_loader._assert_canonical_replay_baseline({})


def test_snowflake_canonical_replay_checks_declared_snapshot_count(monkeypatch):
    rows = [
        ("123:1", "123", 1, datetime(2026, 10, 27, 1, tzinfo=UTC), source, "succeeded")
        for source in ("active_permits", "cleared_permits")
    ]
    connection = _FakeConnection(rows)
    monkeypatch.setattr(
        snowflake_loader.snowflake.connector,
        "connect",
        lambda **kwargs: connection,
    )
    _set_contract(monkeypatch, count="28")
    with pytest.raises(RuntimeError, match="expected 28 complete snapshots"):
        snowflake_loader._assert_canonical_replay_baseline({})


def test_snowflake_canonical_replay_accepts_matching_baseline(monkeypatch):
    rows = [
        ("123:1", "123", 1, datetime(2026, 10, 27, 1, tzinfo=UTC), source, "succeeded")
        for source in ("active_permits", "cleared_permits")
    ]
    connection = _FakeConnection(rows)
    monkeypatch.setattr(
        snowflake_loader.snowflake.connector,
        "connect",
        lambda **kwargs: connection,
    )
    _set_contract(monkeypatch, count="1")
    snowflake_loader._assert_canonical_replay_baseline({})


def test_direct_canonical_duckdb_ingestion_is_rejected():
    from pathlib import Path
    from types import SimpleNamespace

    root = Path(ingestion.__file__).resolve().parents[2]
    app = SimpleNamespace(
        warehouse_kind="duckdb",
        warehouse_credentials=str(root / "data/warehouse/toronto.duckdb"),
        dataset_name="raw",
    )
    with pytest.raises(
        RuntimeError, match="Direct toronto-ingest writes to the canonical DuckDB"
    ):
        ingestion._reject_direct_canonical_load(app, extract_only=False)


def test_direct_snowflake_raw_ingestion_is_rejected():
    from types import SimpleNamespace

    app = SimpleNamespace(
        warehouse_kind="snowflake",
        warehouse_credentials="snowflake://user@account/",
        dataset_name="RAW",
    )
    with pytest.raises(RuntimeError, match="canonical Snowflake TORONTO.RAW"):
        ingestion._reject_direct_canonical_load(app, extract_only=False)


def test_extract_only_ingestion_remains_available():
    from types import SimpleNamespace

    app = SimpleNamespace(
        warehouse_kind="snowflake",
        warehouse_credentials="snowflake://user@account/",
        dataset_name="RAW",
    )
    ingestion._reject_direct_canonical_load(app, extract_only=True)


def test_workflows_require_enabled_flag_and_baseline_contract():
    from pathlib import Path

    cd = Path(".github/workflows/cd.yml").read_text(encoding="utf-8")
    snowflake = Path(".github/workflows/snowflake-canonical-raw-refresh.yml").read_text(
        encoding="utf-8"
    )
    for content in (cd, snowflake):
        assert "TORONTO_CANONICAL_REPLAY_ENABLED" in content
        assert "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID" in content
        assert "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT" in content

    assert "inputs.confirm_canonical_raw == true" in snowflake
    assert "github.event_name == 'workflow_dispatch' &&" in snowflake
    assert "steps.duckdb_snapshot_replay.outcome == 'success'" in cd
    assert "always() && steps.duckdb_snapshot_replay.outcome != 'skipped'" not in cd
