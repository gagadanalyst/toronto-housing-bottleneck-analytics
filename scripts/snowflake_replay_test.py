"""Replay real recovered DuckDB snapshots through the Snowflake dlt SCD2 loader.

This is an isolated migration gate. It reads current rows from the October 23
recovery database and loads each source twice into uniquely named tables in the
SNOWFLAKE_SMOKE schema. It fails if identical replays change SCD2 row counts.
"""

from __future__ import annotations

import os
import re
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import dlt
import duckdb
import pyarrow.parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2


_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _identifier(value: str) -> str:
    """Validate identifiers before interpolating them into SQL."""
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _quoted_identifier(value: str) -> str:
    """Quote a DuckDB identifier safely."""
    return '"' + value.replace('"', '""') + '"'


def _export_current_snapshot(
    *, source_db: Path, source_table: str, output_path: Path
) -> int:
    """Export current business columns from a recovered DuckDB SCD2 table."""
    table = _identifier(source_table)
    with duckdb.connect(str(source_db), read_only=True) as connection:
        described = connection.execute(
            f'DESCRIBE SELECT * FROM "raw".{_quoted_identifier(table)}'
        ).fetchall()
        columns = [str(row[0]) for row in described]
        by_lower = {column.lower(): column for column in columns}
        valid_to = by_lower.get("_dlt_valid_to")
        if valid_to is None:
            raise RuntimeError(
                f"raw.{table} is not an SCD2 table: _dlt_valid_to is missing."
            )

        # Strip destination metadata and the loader's derived hash so this is a
        # source-like business snapshot. The loader re-computes row_hash below.
        business_columns = [
            column
            for column in columns
            if not column.lower().startswith("_dlt_")
            and column.lower() != "row_hash"
        ]
        if not business_columns:
            raise RuntimeError(f"raw.{table} has no business columns to replay.")

        projection = ", ".join(_quoted_identifier(c) for c in business_columns)
        arrow_table = connection.execute(
            f'SELECT {projection} FROM "raw".{_quoted_identifier(table)} '
            f'WHERE {_quoted_identifier(valid_to)} IS NULL'
        ).fetch_arrow_table()

    if arrow_table.num_rows == 0:
        raise RuntimeError(f"raw.{table} has no current rows to replay.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_table, output_path, compression="zstd")
    print(
        f"SNAPSHOT {table}: exported {arrow_table.num_rows:,} current rows "
        f"and {len(business_columns)} business columns to {output_path.name}"
    )
    return int(arrow_table.num_rows)


def _counts(pipeline: dlt.Pipeline, table_name: str) -> tuple[int, int, int]:
    """Return total, current, and expired SCD2 row counts."""
    table = _identifier(table_name)
    with pipeline.sql_client() as client:
        row = client.execute_sql(
            f"""
            SELECT
                COUNT(*),
                COUNT_IF(_dlt_valid_to IS NULL),
                COUNT_IF(_dlt_valid_to IS NOT NULL)
            FROM {table}
            """
        )[0]
    return int(row[0]), int(row[1] or 0), int(row[2] or 0)


def _load_twice(
    *,
    credentials: str,
    dataset_name: str,
    base_source,
    snapshot_path: Path,
    run_token: str,
) -> None:
    """Load a real snapshot twice and assert identical SCD2 counts."""
    source_name = _identifier(f"replay_{base_source.name}_{run_token}")
    table_name = _identifier(f"replay_{base_source.warehouse_table}_{run_token}")
    source = replace(
        base_source,
        name=source_name,
        warehouse_table=table_name,
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"toronto_{source.name}_scd2",
        destination=dlt.destinations.snowflake(credentials=credentials),
        dataset_name=dataset_name,
        dev_mode=False,
    )

    try:
        first = load_scd2(
            credentials=credentials,
            snapshot_path=snapshot_path,
            source=source,
            dataset_name=dataset_name,
            destination_kind="snowflake",
        )
        first.load_info.raise_on_failed_jobs()
        first_counts = _counts(pipeline, table_name)
        if first_counts[0] == 0 or first_counts[1] == 0:
            raise AssertionError(
                f"{base_source.name}: first load produced no SCD2 rows: "
                f"{first_counts}."
            )

        second = load_scd2(
            credentials=credentials,
            snapshot_path=snapshot_path,
            source=source,
            dataset_name=dataset_name,
            destination_kind="snowflake",
        )
        second.load_info.raise_on_failed_jobs()
        second_counts = _counts(pipeline, table_name)

        if second_counts != first_counts:
            raise AssertionError(
                f"{base_source.name}: identical replay changed SCD2 counts. "
                f"After first load (total, current, expired)={first_counts}; "
                f"after second load={second_counts}."
            )

        print(
            f"PASS {base_source.name}: unchanged replay added no SCD2 versions; "
            f"total={first_counts[0]:,}, current={first_counts[1]:,}, "
            f"expired={first_counts[2]:,} after both loads."
        )
    finally:
        # Only this run's uniquely named replay table is removed.
        try:
            with pipeline.sql_client() as client:
                client.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as exc:
            print(
                f"WARNING: could not drop replay table {table_name}: "
                f"{type(exc).__name__}: {exc}"
            )


def main() -> None:
    """Export the recovery snapshot and run both real-snapshot replays."""
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    dataset_name = os.environ.get("TORONTO_WAREHOUSE_DATASET", "SNOWFLAKE_SMOKE")
    warehouse_kind = os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower()
    source_db = Path(os.environ.get("SOURCE_DB", "data/toronto.duckdb"))

    if warehouse_kind != "snowflake":
        raise SystemExit("Set TORONTO_WAREHOUSE_KIND=snowflake for this test.")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")
    # Hard guard: this test must never write to the production RAW schema.
    if dataset_name.upper() != "SNOWFLAKE_SMOKE":
        raise SystemExit(
            "Safety check failed: TORONTO_WAREHOUSE_DATASET must be "
            "SNOWFLAKE_SMOKE; refusing to run against any other schema."
        )
    if not source_db.is_file():
        raise SystemExit(f"Recovery DuckDB file was not found: {source_db}")

    app = load_config(Path("open_data_config.yml"))
    sources = {source.name: source for source in app.sources}
    run_token = re.sub(r"[^A-Za-z0-9_]", "", os.environ.get("GITHUB_RUN_ID", ""))
    if not run_token:
        run_token = uuid4().hex[:10]

    with TemporaryDirectory(prefix="snowflake-replay-") as directory:
        temp = Path(directory)
        for source_name in ("active_permits", "cleared_permits"):
            source = sources[source_name]
            snapshot = temp / f"{source_name}.parquet"
            _export_current_snapshot(
                source_db=source_db,
                source_table=source.warehouse_table,
                output_path=snapshot,
            )
            _load_twice(
                credentials=credentials,
                dataset_name=_identifier(dataset_name),
                base_source=source,
                snapshot_path=snapshot,
                run_token=run_token,
            )

    print(
        "SNOWFLAKE REAL-SNAPSHOT REPLAY PASSED: Active and Cleared current "
        "snapshots each loaded twice with stable total/current/expired SCD2 counts."
    )


if __name__ == "__main__":
    main()
