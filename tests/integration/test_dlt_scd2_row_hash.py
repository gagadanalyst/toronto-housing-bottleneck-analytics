from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from toronto_housing_ingestion.config import SourceConfig
from toronto_housing_ingestion.dlt_loader import load_scd2


def _write_snapshot(
    path: Path,
    *,
    source_id: int,
    builder_name: str,
) -> None:
    """Write one permit row to a Parquet snapshot."""
    table = pa.table(
        {
            "_id": [source_id],
            "PERMIT_NUM": ["21 154961 BLD"],
            "REVISION_NUM": ["00"],
            "PERMIT_TYPE": ["New Houses"],
            "BUILDER_NAME": [builder_name],
            "APPLICATION_DATE": ["2021-05-14"],
            "ISSUED_DATE": ["2021-07-28"],
        }
    )
    pq.write_table(table, path)


def test_scd2_ignores_unstable_source_id_but_tracks_builder_change(
    tmp_path: Path,
) -> None:
    """Verify _id changes do not create a version, but builder changes do."""
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    third = tmp_path / "third.parquet"
    database = tmp_path / "test.duckdb"

    _write_snapshot(
        first,
        source_id=2492,
        builder_name="NOELLE BUTLER",
    )
    _write_snapshot(
        second,
        source_id=1942,
        builder_name="NOELLE BUTLER",
    )
    _write_snapshot(
        third,
        source_id=3000,
        builder_name="JACK KEOUGH",
    )

    source = SourceConfig(
        name="scd2_row_hash",
        title="SCD2 row-hash test",
        dataset_slug="test",
        role="test",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=(
            "_id",
            "PERMIT_NUM",
            "REVISION_NUM",
            "PERMIT_TYPE",
            "BUILDER_NAME",
            "APPLICATION_DATE",
            "ISSUED_DATE",
        ),
        date_columns=("APPLICATION_DATE", "ISSUED_DATE"),
        pagination_sort="PERMIT_NUM asc",
        warehouse_table="scd2_row_hash",
        warehouse_load=True,
        primary_key=(
            "PERMIT_NUM",
            "REVISION_NUM",
            "PERMIT_TYPE",
        ),
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
        total_after_id_change = connection.execute(
            "SELECT COUNT(*) FROM raw.scd2_row_hash"
        ).fetchone()[0]

        current_after_id_change = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_row_hash
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]

    assert total_after_id_change == 1
    assert current_after_id_change == 1

    load_scd2(
        credentials=str(database),
        snapshot_path=third,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    with duckdb.connect(str(database), read_only=True) as connection:
        total_after_builder_change = connection.execute(
            "SELECT COUNT(*) FROM raw.scd2_row_hash"
        ).fetchone()[0]

        current_after_builder_change = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_row_hash
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]

        historical_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.scd2_row_hash
            WHERE _dlt_valid_to IS NOT NULL
            """
        ).fetchone()[0]

        current_builder = connection.execute(
            """
            SELECT BUILDER_NAME
            FROM raw.scd2_row_hash
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]

    assert total_after_builder_change == 2
    assert current_after_builder_change == 1
    assert historical_rows == 1
    assert current_builder == "JACK KEOUGH"
