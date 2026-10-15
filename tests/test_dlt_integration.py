from pathlib import Path

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import pandas as pd

from toronto_housing_ingestion.config import SourceConfig
from toronto_housing_ingestion.dlt_loader import load_scd2


def test_scd2_same_snapshot_is_idempotent(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    snapshot = tmp_path / "snapshot.parquet"
    table = pa.Table.from_pandas(
        pd.DataFrame({"id": [1, 2], "status": ["open", "open"]}),
        preserve_index=False,
    )
    pq.write_table(table, snapshot)

    source = SourceConfig(
        name="test",
        title="Test",
        dataset_slug="test",
        role="test",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=("id",),
        date_columns=(),
        pagination_sort="id asc",
        warehouse_table="test",
        warehouse_load=True,
        primary_key=("id",),
        type_hints={},
    )
    db = tmp_path / "test.duckdb"
    result1 = load_scd2(
        credentials=str(db),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    result2 = load_scd2(
        credentials=str(db),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )
    assert result1.current_rows == 2
    assert result2.total_rows == 2
    assert result2.current_rows == 2

def test_warehouse_cutoff_filters_load_not_raw_snapshot(tmp_path: Path) -> None:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    snapshot = tmp_path / "cutoff_snapshot.parquet"
    table = pa.Table.from_pandas(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "APPLICATION_DATE": [
                    "2019-12-31",
                    "2020-01-01",
                    "2024-05-01",
                ],
            }
        ),
        preserve_index=False,
    )
    pq.write_table(table, snapshot)

    source = SourceConfig(
        name="cutoff_test",
        title="Cutoff Test",
        dataset_slug="cutoff-test",
        role="test",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=("id", "APPLICATION_DATE"),
        date_columns=("APPLICATION_DATE",),
        pagination_sort="id asc",
        warehouse_table="cutoff_permits",
        warehouse_load=True,
        primary_key=("id",),
        type_hints={},
        warehouse_since="2020-01-01",
        warehouse_date_column="APPLICATION_DATE",
    )

    db = tmp_path / "cutoff.duckdb"
    result = load_scd2(
        credentials=str(db),
        snapshot_path=snapshot,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
    )

    # Raw extraction retains all three rows, including the 2019 record.
    assert pq.read_table(snapshot).column("id").to_pylist() == [1, 2, 3]

    # The warehouse receives only the two records on or after the cutoff.
    assert result.current_rows == 2

    with duckdb.connect(str(db), read_only=True) as connection:
        count = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.cutoff_permits
            WHERE _dlt_valid_to IS NULL
            """
        ).fetchone()[0]
        assert count == 2
        pre_cutoff_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM raw.cutoff_permits
            WHERE id = 1 AND _dlt_valid_to IS NULL
            """
        ).fetchone()[0]
        assert pre_cutoff_count == 0