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
