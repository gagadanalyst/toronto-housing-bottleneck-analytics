"""Verify Snowflake connectivity and dlt SCD2 change detection with synthetic data."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import dlt
import pyarrow as pa
import pyarrow.parquet as pq

from toronto_housing_ingestion.config import SourceConfig
from toronto_housing_ingestion.dlt_loader import load_scd2


def main() -> None:
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    dataset = os.environ.get("TORONTO_WAREHOUSE_DATASET", "SNOWFLAKE_SMOKE")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")
    if os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower() != "snowflake":
        raise SystemExit("This test must run with TORONTO_WAREHOUSE_KIND=snowflake.")

    token = uuid4().hex[:8]
    source_name = f"snowflake_smoke_{token}"
    table_name = f"smoke_{token}"
    pipeline_name = f"toronto_{source_name}_scd2"

    source = SourceConfig(
        name=source_name,
        title="Synthetic Snowflake SCD2 smoke test",
        dataset_slug="synthetic-snowflake-smoke",
        role="test",
        expected_cadence="manual",
        portal_url="https://example.invalid/synthetic-smoke-test",
        required_columns=(
            "permit_num",
            "revision_num",
            "permit_type",
            "status",
            "marker",
        ),
        date_columns=(),
        pagination_sort="_id asc",
        warehouse_table=table_name,
        warehouse_load=True,
        primary_key=("permit_num", "revision_num", "permit_type"),
        type_hints={},
        warehouse_strategy="scd2",
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.snowflake(credentials=credentials),
        dataset_name=dataset,
        dev_mode=False,
    )

    def write_snapshot(path: Path, marker: str, source_id: int) -> None:
        # _id changes between the first two snapshots; it must not create a version.
        row = {
            "_id": source_id,
            "permit_num": "SMOKE-0001",
            "revision_num": 1,
            "permit_type": "SMOKE_TEST",
            "status": "open",
            "marker": marker,
        }
        pq.write_table(pa.Table.from_pylist([row]), path)

    def current_counts() -> tuple[int, int]:
        with pipeline.sql_client() as client:
            result = client.execute_sql(
                f"""
                SELECT COUNT(*), COUNT_IF(_dlt_valid_to IS NULL)
                FROM {table_name}
                """
            )
        return int(result[0][0]), int(result[0][1])

    try:
        with TemporaryDirectory(prefix="snowflake-smoke-") as temp_dir:
            temp = Path(temp_dir)

            snapshots = [
                (temp / "initial.parquet", "version_1", 101),
                (temp / "unchanged.parquet", "version_1", 999),
                (temp / "changed.parquet", "version_2", 1000),
            ]

            for path, marker, source_id in snapshots:
                write_snapshot(path, marker, source_id)
                result = load_scd2(
                    credentials=credentials,
                    snapshot_path=path,
                    source=source,
                    dataset_name=dataset,
                    destination_kind="snowflake",
                )
                result.load_info.raise_on_failed_jobs()

            counts = current_counts()
            if counts != (2, 1):
                raise AssertionError(
                    f"Expected 2 historical rows and 1 current row; got {counts}."
                )

            with pipeline.sql_client() as client:
                expired = client.execute_sql(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE _dlt_valid_to IS NOT NULL
                    """
                )[0][0]

            if int(expired) != 1:
                raise AssertionError(f"Expected 1 expired SCD2 version; got {expired}.")

            print(
                "SNOWFLAKE SCD2 SMOKE TEST PASSED: "
                "initial load, unchanged snapshot, changed snapshot, "
                "historical version and current-row checks."
            )
    finally:
        # The test table is temporary by convention and is removed after the run.
        try:
            with pipeline.sql_client() as client:
                client.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as exc:
            print(
                "WARNING: smoke-test cleanup could not remove "
                f"{table_name}: {type(exc).__name__}: {exc}"
            )


if __name__ == "__main__":
    main()
