from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from toronto_housing_ingestion.config import SourceConfig
from toronto_housing_ingestion.dlt_loader import load_scd2


def _write_snapshot(path: Path, permits: list[tuple[str, int, str]]) -> None:
    """Write a small full snapshot using the same natural-key shape as permits."""
    table = pa.table(
        {
            "PERMIT_NUM": [row[0] for row in permits],
            "REVISION_NUM": [row[1] for row in permits],
            "PERMIT_TYPE": [row[2] for row in permits],
        }
    )
    pq.write_table(table, path)


def test_scd2_retires_row_missing_from_full_snapshot(tmp_path: Path) -> None:
    """Verify SCD2 closes a row absent from a later full snapshot."""
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    database = tmp_path / "test.duckdb"

    _write_snapshot(
        first,
        [
            ("A", 1, "NEW"),
            ("B", 1, "NEW"),
            ("C", 1, "NEW"),
        ],
    )
    _write_snapshot(
        second,
        [
            ("A", 1, "NEW"),
            ("C", 1, "NEW"),
        ],
    )

    source = SourceConfig(
        name="scd2_missing_row",
        title="SCD2 missing-row test",
        dataset_slug="test",
        role="test",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
        date_columns=(),
        pagination_sort="PERMIT_NUM asc",
        warehouse_table="scd2_missing_row",
        warehouse_load=True,
        primary_key=("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
        type_hints={},
    )

    load_scd2(
        credentials=str(database),
        snapshot_path=first,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    load_scd2(
        credentials=str(database),
        snapshot_path=second,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    with duckdb.connect(str(database), read_only=True) as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM raw.scd2_missing_row"
        ).fetchone()[0]
        current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_missing_row
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]
        deleted_current = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_missing_row
            WHERE PERMIT_NUM = 'B'
              AND _dlt_valid_to IS NULL
            """
        ).fetchone()[0]
        deleted_history = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_missing_row
            WHERE PERMIT_NUM = 'B'
              AND _dlt_valid_to IS NOT NULL
            """
        ).fetchone()[0]

    assert total == 3
    assert current == 2
    assert deleted_current == 0
    assert deleted_history == 1
