"""Validate that configured permit sources were loaded into DuckDB."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

SOURCES = ("active_permits", "cleared_permits")
WAREHOUSE_PATH = Path("data/warehouse/toronto.duckdb")


def _latest_manifest(source_name: str) -> Path | None:
    paths = sorted(
        Path("data/manifests", source_name).glob("*.json"),
        reverse=True,
    )
    return paths[0] if paths else None


@pytest.mark.parametrize("source_name", SOURCES)
def test_permit_source_loaded_to_duckdb(source_name: str) -> None:
    """Require each permit source to be successfully loaded."""
    manifest_path = _latest_manifest(source_name)

    if manifest_path is None:
        pytest.fail(f"No manifest found for {source_name}.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["warehouse_status"] == "loaded", (
        f"{source_name} warehouse load status: {manifest['warehouse_status']}"
    )

    assert WAREHOUSE_PATH.is_file(), f"DuckDB warehouse not found: {WAREHOUSE_PATH}"

    table_name = manifest.get("warehouse_table")
    assert table_name, f"No warehouse table recorded for {source_name}."

    with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as connection:
        exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'raw'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]

        assert exists == 1, f"Missing DuckDB table: raw.{table_name}"

        row_count = connection.execute(
            f"SELECT COUNT(*) FROM raw.{table_name}"
        ).fetchone()[0]

    assert row_count > 0, f"DuckDB table raw.{table_name} is empty."
