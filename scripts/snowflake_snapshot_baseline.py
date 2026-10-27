"""Load one validated snapshot bundle into a fresh Snowflake SCD2 baseline schema."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from zipfile import ZipFile

import dlt
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2

SOURCE_NAMES = ("active_permits", "cleared_permits")
DATASET_PATTERN = re.compile(r"SNOWFLAKE_BASELINE_[0-9]+")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_bytes(value: bytes, label: str) -> dict:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return parsed


def _count_rows(pipeline: dlt.Pipeline, table_name: str) -> tuple[int, int, int]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Unsafe table identifier: {table_name!r}")
    with pipeline.sql_client() as client:
        row = client.execute_sql(
            f"SELECT COUNT(*), COUNT_IF(_dlt_valid_to IS NULL), "
            f"COUNT_IF(_dlt_valid_to IS NOT NULL) FROM {table_name}"
        )[0]
    return int(row[0]), int(row[1] or 0), int(row[2] or 0)


def _validate_and_extract(bundle_path: Path, destination: Path, sources: dict) -> dict:
    expected_members = {"bundle.json"}
    for name in SOURCE_NAMES:
        expected_members.update({f"{name}/data.parquet", f"{name}/manifest.json"})

    with ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
        if names != expected_members:
            raise ValueError(
                f"Bundle contents differ from the expected five files. "
                f"Missing={sorted(expected_members - names)}, "
                f"extra={sorted(names - expected_members)}"
            )
        bundle = _read_json_bytes(archive.read("bundle.json"), "bundle.json")
        if bundle.get("format_version") != 1:
            raise ValueError("Unsupported snapshot bundle format version.")
        expected_run = os.environ.get("SNAPSHOT_SOURCE_RUN_ID", "")
        expected_commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
        if not expected_run or str(bundle.get("source_run_id")) != expected_run:
            raise ValueError("Bundle run ID does not match the selected CD run.")
        if not expected_commit or bundle.get("source_commit") != expected_commit:
            raise ValueError("Bundle commit does not match the selected CD run.")

        destination.mkdir(parents=True, exist_ok=True)
        for source_name in SOURCE_NAMES:
            source = sources.get(source_name)
            summary = bundle.get("sources", {}).get(source_name)
            if source is None or not isinstance(summary, dict):
                raise ValueError(f"Bundle is missing configured source {source_name}.")
            snapshot_name = f"{source_name}/data.parquet"
            manifest_name = f"{source_name}/manifest.json"
            snapshot_path = destination / source_name / "data.parquet"
            manifest_path = destination / source_name / "manifest.json"
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            with (
                archive.open(snapshot_name) as source_stream,
                snapshot_path.open("wb") as target_stream,
            ):
                shutil.copyfileobj(source_stream, target_stream)
            manifest_path.write_bytes(archive.read(manifest_name))

            manifest = _read_json_bytes(manifest_path.read_bytes(), manifest_name)
            digest = _sha256(snapshot_path)
            if digest != summary.get("sha256") or digest != manifest.get("sha256"):
                raise ValueError(
                    f"{source_name}: snapshot SHA-256 verification failed."
                )
            parquet = pq.ParquetFile(snapshot_path)
            count = int(parquet.metadata.num_rows)
            columns = list(parquet.schema_arrow.names)
            if count != summary.get("row_count") or count != manifest.get("row_count"):
                raise ValueError(f"{source_name}: snapshot row counts disagree.")
            if sorted(columns) != sorted(summary.get("columns", [])):
                raise ValueError(
                    f"{source_name}: snapshot columns disagree with bundle manifest."
                )
            column_set = {column.lower() for column in columns}
            missing = sorted(
                name
                for name in source.required_columns
                if name.lower() not in column_set
            )
            if missing:
                raise ValueError(f"{source_name}: missing required columns: {missing}")
            if (
                manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "loaded"
            ):
                raise ValueError(
                    f"{source_name}: source manifest is not a successful load."
                )
            if manifest.get("warehouse_since") != source.warehouse_since:
                raise ValueError(
                    f"{source_name}: warehouse date cutoff differs from configuration."
                )
            if manifest.get("warehouse_date_column") != source.warehouse_date_column:
                raise ValueError(
                    f"{source_name}: warehouse date column differs from configuration."
                )
            print(f"Verified {source_name}: {count:,} source rows, sha256={digest}")

    return bundle


def main() -> None:
    bundle_path = Path(
        os.environ.get(
            "SNAPSHOT_BUNDLE_PATH",
            "data/snapshot-download/toronto-snapshot-bundle.zip",
        )
    )
    dataset = os.environ.get("TORONTO_WAREHOUSE_DATASET", "").upper()
    if not DATASET_PATTERN.fullmatch(dataset):
        raise ValueError("Refusing to load outside SNOWFLAKE_BASELINE_<source-run-id>.")

    app = load_config(Path("open_data_config.yml"))
    if app.warehouse_kind != "snowflake" or not app.warehouse_credentials:
        raise ValueError(
            "Snowflake warehouse configuration and credentials are required."
        )
    sources = {source.name: source for source in app.sources}
    for name in SOURCE_NAMES:
        source = sources.get(name)
        if (
            source is None
            or not source.warehouse_load
            or source.warehouse_strategy != "scd2"
        ):
            raise ValueError(f"{name}: expected an enabled SCD2 source configuration.")

    with tempfile.TemporaryDirectory(prefix="toronto-snowflake-baseline-") as temp:
        bundle = _validate_and_extract(bundle_path, Path(temp), sources)
        probe = dlt.pipeline(
            pipeline_name=f"toronto_baseline_probe_{dataset.lower()}",
            destination=dlt.destinations.snowflake(
                credentials=app.warehouse_credentials, use_timestamp_tz=False
            ),
            dataset_name=dataset,
            dev_mode=False,
        )

        baseline_counts: dict[str, tuple[int, int, int]] = {}
        for name in SOURCE_NAMES:
            source = sources[name]
            snapshot_path = Path(temp) / name / "data.parquet"
            load_scd2(
                credentials=app.warehouse_credentials,
                snapshot_path=snapshot_path,
                source=source,
                dataset_name=dataset,
                destination_kind="snowflake",
            )
            counts = _count_rows(probe, source.warehouse_table)
            if counts[0] < 1 or counts[1] < 1 or counts[0] != counts[1] + counts[2]:
                raise ValueError(
                    f"{name}: initial SCD2 counts do not reconcile: {counts}"
                )
            baseline_counts[name] = counts
            print(
                f"BASELINE {name}: total={counts[0]:,}, current={counts[1]:,}, "
                f"expired={counts[2]:,}",
                flush=True,
            )

        for name in SOURCE_NAMES:
            source = sources[name]
            load_scd2(
                credentials=app.warehouse_credentials,
                snapshot_path=Path(temp) / name / "data.parquet",
                source=source,
                dataset_name=dataset,
                destination_kind="snowflake",
            )
            after = _count_rows(probe, source.warehouse_table)
            if after != baseline_counts[name]:
                raise ValueError(
                    f"{name}: replay changed SCD2 counts. "
                    f"baseline={baseline_counts[name]}, replay={after}"
                )
            print(
                f"PASS {name}: unchanged snapshot replay added no SCD2 versions.",
                flush=True,
            )

    print(
        f"SNOWFLAKE SNAPSHOT BASELINE PASSED: source run {bundle['source_run_id']} "
        f"loaded into TORONTO.{dataset}; the identical replay added no SCD2 versions.",
        flush=True,
    )


if __name__ == "__main__":
    main()
