"""SCD2 idempotency when an immutable archive is retried or repeated later."""

from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from toronto_housing_ingestion.dlt_loader import load_scd2


def test_replaying_same_snapshot_then_later_unchanged_snapshot_adds_no_history(
    tmp_path,
):
    database = tmp_path / "replay.duckdb"
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    rows = [
        {
            "_id": 101,
            "PERMIT_NUM": "26 184407 BLD",
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "New Houses",
            "BUILDER_NAME": "STANLEY MP INC",
        }
    ]
    pq.write_table(pa.Table.from_pylist(rows), first)
    pq.write_table(pa.Table.from_pylist(rows), second)
    source = SimpleNamespace(
        name="active_permits",
        primary_key=("permit_num", "revision_num", "permit_type"),
        warehouse_date_column=None,
        warehouse_since=None,
        warehouse_load=True,
        warehouse_strategy="scd2",
        warehouse_table="raw_active_permits",
        type_hints={},
    )
    pipeline = f"test_snapshot_replay_{tmp_path.name}"
    first_boundary = "2026-11-06T15:00:00+00:00"
    later_boundary = "2026-11-07T15:00:00+00:00"

    # A retry of the exact same archive must keep the same SCD2 boundary.
    for _ in range(2):
        load_scd2(
            credentials=str(database),
            snapshot_path=first,
            source=source,
            dataset_name="raw",
            destination_kind="duckdb",
            pipeline_name=pipeline,
            boundary_timestamp=first_boundary,
        )

    # A distinct later extraction containing unchanged data must not add a version.
    load_scd2(
        credentials=str(database),
        snapshot_path=second,
        source=source,
        dataset_name="raw",
        destination_kind="duckdb",
        pipeline_name=pipeline,
        boundary_timestamp=later_boundary,
    )

    with duckdb.connect(str(database), read_only=True) as connection:
        total, current = connection.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE _dlt_valid_to IS NULL) "
            "FROM raw.raw_active_permits "
            "WHERE permit_num = '26 184407 BLD' "
            "AND revision_num = '00' AND permit_type = 'New Houses'"
        ).fetchone()
    assert (total, current) == (1, 1)
