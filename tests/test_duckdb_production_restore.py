from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from scripts import restore_duckdb_production as restore


def _database(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA raw")

        for table in ("raw_active_permits", "raw_cleared_permits"):
            connection.execute(f"CREATE TABLE raw.{table} (_dlt_valid_to TIMESTAMP)")
            connection.execute(f"INSERT INTO raw.{table} VALUES (NULL)")

        connection.execute(
            "CREATE TABLE raw.toronto_snapshot_ledger (snapshot_id VARCHAR)"
        )
        connection.execute(
            "INSERT INTO raw.toronto_snapshot_ledger VALUES ('snapshot-1')"
        )


def test_warehouse_latest_is_the_authoritative_restore_source(tmp_path, monkeypatch):
    source = tmp_path / "source.duckdb"
    _database(source)

    def download(_repository, destination, _release_tag, _database_asset):
        shutil.copy2(source, destination)

    monkeypatch.setattr(restore, "_rolling_asset_available", lambda _repository, _tag, _asset: True)
    monkeypatch.setattr(restore, "_download_rolling_asset", download)
    monkeypatch.setattr(
        restore,
        "_restore_immutable_checkpoint",
        lambda *_args: pytest.fail("Checkpoint fallback must not be used."),
    )

    output = tmp_path / "restored" / "toronto.duckdb"
    result = restore.restore_production_database("owner/repo", output)

    assert result == "warehouse-latest"
    assert restore._sha256(output) == restore._sha256(source)


def test_missing_rolling_asset_uses_the_immutable_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(restore, "_rolling_asset_available", lambda _repository, _tag, _asset: False)

    def checkpoint(_repository, destination):
        _database(destination)
        return True

    monkeypatch.setattr(restore, "_restore_immutable_checkpoint", checkpoint)

    output = tmp_path / "restored" / "toronto.duckdb"
    result = restore.restore_production_database("owner/repo", output)

    assert result == "immutable-recovery-checkpoint"
    restore._validate_database(output)


def test_invalid_present_rolling_asset_fails_closed_without_fallback(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(restore, "_rolling_asset_available", lambda _repository, _tag, _asset: True)

    def invalid_download(_repository, destination, _release_tag, _database_asset):
        destination.write_bytes(b"not a DuckDB database")

    fallback_called = []

    monkeypatch.setattr(restore, "_download_rolling_asset", invalid_download)
    monkeypatch.setattr(
        restore,
        "_restore_immutable_checkpoint",
        lambda *_args: fallback_called.append(True),
    )

    with pytest.raises(RuntimeError):
        restore.restore_production_database(
            "owner/repo",
            tmp_path / "restored" / "toronto.duckdb",
        )

    assert fallback_called == []


def test_restore_helper_requires_current_rows_and_snapshot_ledger(tmp_path):
    incomplete = tmp_path / "incomplete.duckdb"

    with duckdb.connect(str(incomplete)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            "CREATE TABLE raw.raw_active_permits (_dlt_valid_to TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE raw.raw_cleared_permits (_dlt_valid_to TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE raw.toronto_snapshot_ledger (snapshot_id VARCHAR)"
        )
        connection.execute("INSERT INTO raw.raw_active_permits VALUES (NULL)")
        connection.execute(
            "INSERT INTO raw.toronto_snapshot_ledger VALUES ('snapshot-1')"
        )

    with pytest.raises(RuntimeError, match="raw.raw_cleared_permits"):
        restore._validate_database(incomplete)


def test_mode1_mode2_and_promotion_use_the_same_restore_helper():
    root = Path(__file__).resolve().parents[1]

    mode1 = (root / ".github/workflows/mode1-incremental-loader.yml").read_text(
        encoding="utf-8"
    )
    mode2 = (root / ".github/workflows/snapshot-replay.yml").read_text(encoding="utf-8")
    promotion = (root / ".github/workflows/promote-snapshot-rebuild.yml").read_text(
        encoding="utf-8"
    )

    command = "python -m scripts.restore_duckdb_production"

    assert mode1.count(command) == 1
    assert mode2.count(command) == 1
    assert promotion.count(command) == 2


def test_loading_restore_refuses_warehouse_checkpoint_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(restore, "_rolling_asset_available", lambda *_args: False)
    called = []
    monkeypatch.setattr(
        restore, "_restore_immutable_checkpoint",
        lambda *_args: called.append(True),
    )
    with pytest.raises(FileNotFoundError, match="loading-latest"):
        restore.restore_production_database(
            "owner/repo", tmp_path / "loading.duckdb",
            release_tag="loading-latest",
            database_asset="toronto-loading.duckdb",
        )
    assert called == []
