"""Run the real Toronto extraction and dlt load twice in an isolated Snowflake schema.

The first pass uses the actual CLI and live CKAN source. Subsequent passes load
exactly the same extracted Parquet snapshots with --local-load and assert that
SCD2 total/current/expired counts do not change. A per-run schema is dropped at
the end so this gate cannot touch TORONTO.RAW or the shared SNOWFLAKE_SMOKE schema.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import dlt

from toronto_housing_ingestion.config import load_config


def _safe_dataset(value: str) -> str:
    """Accept only the dedicated per-run schema naming convention."""
    normalized = value.upper()
    if not re.fullmatch(r"SNOWFLAKE_INGEST_SMOKE_[A-Z0-9_]+", normalized):
        raise ValueError(
            "Refusing to run outside a per-run test schema named "
            "SNOWFLAKE_INGEST_SMOKE_<run-id>."
        )
    return normalized


def _count_rows(pipeline: dlt.Pipeline, table_name: str) -> tuple[int, int, int]:
    """Return (total, current, expired) counts for a loaded SCD2 table."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Unsafe table identifier: {table_name!r}")
    with pipeline.sql_client() as client:
        row = client.execute_sql(
            f"""
            SELECT
                COUNT(*),
                COUNT_IF(_dlt_valid_to IS NULL),
                COUNT_IF(_dlt_valid_to IS NOT NULL)
            FROM {table_name}
            """
        )[0]
    return int(row[0]), int(row[1] or 0), int(row[2] or 0)


def _run_cli(*arguments: str) -> None:
    """Run the project CLI in the current workflow checkout."""
    command = [sys.executable, "-m", "toronto_housing_ingestion.ingestion", *arguments]
    print("\n$ " + " ".join(command), flush=True)
    subprocess.run(command, check=True)


def _quote_identifier(value: str) -> str:
    """Quote a Snowflake identifier after checking its shape."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Snowflake identifier: {value!r}")
    return f'"{value}"'


def _columns(client, schema: str, table: str) -> list[str]:
    """Read the ordered column list for one table in TORONTO."""
    schema = schema.upper()
    table = table.upper()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", schema) or not re.fullmatch(
        r"[A-Z_][A-Z0-9_]*", table
    ):
        raise ValueError("Unsafe schema or table name for hash compatibility check.")
    rows = client.execute_sql(
        "SELECT COLUMN_NAME FROM TORONTO.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
        "ORDER BY ORDINAL_POSITION"
    )
    return [str(row[0]) for row in rows]


def _verify_fresh_hash_compatibility(
    pipeline: dlt.Pipeline, dataset: str, source
) -> None:
    """Compare fresh hashes with migrated hashes for rows with identical data.

    The live smoke test already performs a full current CKAN extraction. This
    read-only comparison reuses that load and checks only rows whose tracked
    source attributes are identical in the fresh table and migrated TORONTO.RAW.
    Changed business values are intentionally excluded because their new hash is
    expected to differ. No production table is written.
    """
    table = source.warehouse_table.upper()
    dataset = dataset.upper()
    with pipeline.sql_client() as client:
        fresh_columns = _columns(client, dataset, table)
        migrated_columns = _columns(client, "RAW", table)
        fresh_map = {column.lower(): column for column in fresh_columns}
        migrated_map = {column.lower(): column for column in migrated_columns}

        required = {"row_hash", "_dlt_valid_to", *[key.lower() for key in source.primary_key]}
        for label, column_map in (("fresh", fresh_map), ("migrated", migrated_map)):
            missing = sorted(required - set(column_map))
            if missing:
                raise AssertionError(
                    f"{source.name}: {label} table is missing columns required for "
                    f"hash compatibility validation: {missing}"
                )

        def tracked_columns(column_map: dict[str, str]) -> dict[str, str]:
            return {
                lower: actual
                for lower, actual in column_map.items()
                if lower not in {"_id", "row_hash"}
                and not lower.startswith("_dlt_")
            }

        fresh_tracked = tracked_columns(fresh_map)
        migrated_tracked = tracked_columns(migrated_map)
        fresh_only = sorted(set(fresh_tracked) - set(migrated_tracked))
        migrated_only = sorted(set(migrated_tracked) - set(fresh_tracked))
        if fresh_only or migrated_only:
            raise AssertionError(
                f"{source.name}: cannot establish hash compatibility because source "
                "hash-input schemas differ between the fresh snapshot and migrated RAW. "
                f"Fresh-only columns={fresh_only}; migrated-only columns={migrated_only}. "
                "Review source schema drift before cutover."
            )

        key_columns = [key.lower() for key in source.primary_key]
        key_join = " AND ".join(
            f"fresh.{_quote_identifier(fresh_map[key])} = "
            f"migrated.{_quote_identifier(migrated_map[key])}"
            for key in key_columns
        )
        non_null_keys = " AND ".join(
            f"fresh.{_quote_identifier(fresh_map[key])} IS NOT NULL AND "
            f"migrated.{_quote_identifier(migrated_map[key])} IS NOT NULL"
            for key in key_columns
        )
        same_data = " AND ".join(
            f"fresh.{_quote_identifier(fresh_tracked[column])} IS NOT DISTINCT FROM "
            f"migrated.{_quote_identifier(migrated_tracked[column])}"
            for column in sorted(fresh_tracked)
        )
        fresh_hash = f"fresh.{_quote_identifier(fresh_map['row_hash'])}"
        migrated_hash = f"migrated.{_quote_identifier(migrated_map['row_hash'])}"
        fresh_valid_to = f"fresh.{_quote_identifier(fresh_map['_dlt_valid_to'])}"
        migrated_valid_to = f"migrated.{_quote_identifier(migrated_map['_dlt_valid_to'])}"
        join_and_filter = (
            f"FROM TORONTO.{dataset}.{table} AS fresh "
            f"JOIN TORONTO.RAW.{table} AS migrated ON {key_join} "
            f"WHERE {fresh_valid_to} IS NULL AND {migrated_valid_to} IS NULL "
            f"AND ({non_null_keys}) AND ({same_data})"
        )
        result = client.execute_sql(
            f"SELECT COUNT(*), COUNT_IF({fresh_hash} IS DISTINCT FROM {migrated_hash}) "
            f"{join_and_filter}"
        )[0]
        identical_rows = int(result[0] or 0)
        mismatched_hashes = int(result[1] or 0)

        if identical_rows == 0:
            raise AssertionError(
                f"{source.name}: no current rows had identical tracked attributes in the "
                "fresh snapshot and migrated RAW, so hash compatibility could not be tested."
            )
        if mismatched_hashes:
            key_projection = ", ".join(
                f"fresh.{_quote_identifier(fresh_map[key])}" for key in key_columns
            )
            examples = client.execute_sql(
                f"SELECT {key_projection}, {fresh_hash}, {migrated_hash} "
                f"{join_and_filter} AND ({fresh_hash} IS DISTINCT FROM {migrated_hash}) "
                "LIMIT 5"
            )
            raise AssertionError(
                f"{source.name}: fresh-snapshot row hashes differ from migrated hashes for "
                f"{mismatched_hashes:,} of {identical_rows:,} current rows whose tracked "
                f"attributes are identical. Example (business key, fresh hash, migrated hash): "
                f"{examples!r}. This indicates a hash-input/type-normalization incompatibility."
            )

        print(
            f"PASS fresh-snapshot hash compatibility {source.name}: "
            f"{identical_rows:,} current rows with identical tracked attributes; "
            "zero hash mismatches against TORONTO.RAW.",
            flush=True,
        )


def main() -> None:
    """Extract live snapshots, replay the same files, and assert idempotence."""
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    kind = os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower()
    if kind != "snowflake":
        raise SystemExit("Set TORONTO_WAREHOUSE_KIND=snowflake for this test.")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")

    dataset = _safe_dataset(os.environ.get("TORONTO_WAREHOUSE_DATASET", ""))
    app = load_config(Path("open_data_config.yml"))
    sources = {source.name: source for source in app.sources}
    selected = ("active_permits", "cleared_permits")
    for name in selected:
        source = sources.get(name)
        if source is None or not source.warehouse_load or source.warehouse_strategy != "scd2":
            raise RuntimeError(f"{name}: expected an enabled SCD2 source configuration.")

    # This probe pipeline only supplies a Snowflake SQL client. It does not run
    # any resources, and all queries are scoped to the isolated per-run schema.
    probe = dlt.pipeline(
        pipeline_name=f"toronto_ingestion_gate_probe_{dataset.lower()}",
        destination=dlt.destinations.snowflake(credentials=credentials),
        dataset_name=dataset,
        dev_mode=False,
    )

    try:
        # Perform a fresh, live source extraction and initial Snowflake load.
        source_args = tuple(arg for name in selected for arg in ("--source", name))
        _run_cli("--full-refresh", *source_args)

        baseline: dict[str, tuple[int, int, int]] = {}
        for name in selected:
            source = sources[name]
            counts = _count_rows(probe, source.warehouse_table)
            if counts[0] == 0 or counts[1] == 0:
                raise AssertionError(f"{name}: first load produced no SCD2 rows: {counts}")
            if counts[0] != counts[1] + counts[2]:
                raise AssertionError(f"{name}: SCD2 totals do not reconcile: {counts}")
            baseline[name] = counts
            print(
                f"BASELINE {name}: total={counts[0]:,}, current={counts[1]:,}, "
                f"expired={counts[2]:,}",
                flush=True,
            )
            _verify_fresh_hash_compatibility(probe, dataset, source)

        # Two loads of the identical Parquet snapshots: local-load reuses the
        # most recent successful extraction and does not make another CKAN call.
        for replay_number in (1, 2):
            _run_cli("--local-load", *source_args)
            for name in selected:
                source = sources[name]
                after = _count_rows(probe, source.warehouse_table)
                if after != baseline[name]:
                    raise AssertionError(
                        f"{name}: replay {replay_number} changed SCD2 counts. "
                        f"Baseline (total,current,expired)={baseline[name]}, "
                        f"after replay={after}."
                    )
                print(
                    f"PASS {name}: replay {replay_number} added no SCD2 versions; "
                    f"total={after[0]:,}, current={after[1]:,}, expired={after[2]:,}",
                    flush=True,
                )

        print(
            "SNOWFLAKE LIVE INGESTION SMOKE PASSED: real Active and Cleared "
            "extraction loaded to an isolated schema; two identical replays "
            "did not change SCD2 counts.",
            flush=True,
        )
    finally:
        # Drop only the schema whose name was validated above; never RAW.
        try:
            with probe.sql_client() as client:
                client.execute_sql(f"DROP SCHEMA IF EXISTS TORONTO.{dataset} CASCADE")
            print(f"Cleaned up isolated schema TORONTO.{dataset}.", flush=True)
        except Exception as exc:
            print(
                f"WARNING: unable to drop isolated schema TORONTO.{dataset}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )


if __name__ == "__main__":
    main()
