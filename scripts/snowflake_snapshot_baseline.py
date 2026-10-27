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

from toronto_housing_ingestion.config import SourceConfig, load_config
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


def _prepare_changed_active_snapshots(
    snapshot_path: Path,
    original_path: Path,
    changed_path: Path,
    source: SourceConfig,
    workflow_run_id: str,
    attempt: str,
) -> tuple[tuple[object, ...], str, str]:
    """Write one-row original and changed snapshots for a controlled SCD2 test."""
    import pandas as pd
    import pyarrow as pa

    table = pq.read_table(snapshot_path)
    frame = table.to_pandas()
    columns = {str(name).lower(): str(name) for name in frame.columns}
    key_columns = [columns.get(key.lower()) for key in source.primary_key]
    status_column = columns.get("status")
    date_column = (
        columns.get(source.warehouse_date_column.lower())
        if source.warehouse_date_column
        else None
    )

    if not key_columns or any(column is None for column in key_columns):
        raise ValueError("Active snapshot is missing configured primary-key columns.")
    if status_column is None:
        raise ValueError("Active snapshot is missing STATUS.")
    if source.warehouse_since and date_column is None:
        raise ValueError("Active snapshot is missing the warehouse date column.")

    eligible = pd.Series(True, index=frame.index)
    if source.warehouse_since:
        dates = pd.to_datetime(frame[date_column], errors="coerce", utc=True)
        cutoff = pd.Timestamp(source.warehouse_since, tz="UTC")
        eligible = dates.ge(cutoff)

    candidates = frame.loc[eligible]
    valid_keys = candidates[key_columns].notna().all(axis=1)
    unique_keys = ~candidates.duplicated(subset=key_columns, keep=False)
    valid_status = candidates[status_column].notna() & candidates[status_column].astype(
        str
    ).str.strip().ne("")
    candidates = candidates.loc[valid_keys & unique_keys & valid_status]
    if candidates.empty:
        raise ValueError(
            "No unique Active business key with a non-empty STATUS was found "
            "inside the configured warehouse date window."
        )

    selected_index = candidates.index[0]
    original = frame.loc[[selected_index]].copy()
    changed = original.copy()
    key_values = tuple(
        value.item() if hasattr(value, "item") else value
        for value in original.iloc[0][key_columns].tolist()
    )
    original_status = str(original.iloc[0][status_column])
    test_status = f"SCD2_TEST_{workflow_run_id}_{attempt}"
    if original_status == test_status:
        raise ValueError("Test status unexpectedly matches the original status.")
    changed.loc[changed.index[0], status_column] = test_status

    original_path.parent.mkdir(parents=True, exist_ok=True)
    changed_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(
            original, schema=table.schema, preserve_index=False, safe=False
        ),
        original_path,
    )
    pq.write_table(
        pa.Table.from_pandas(
            changed, schema=table.schema, preserve_index=False, safe=False
        ),
        changed_path,
    )
    print(
        "Prepared one-row controlled SCD2 fixture: "
        f"original status={original_status!r}, test status={test_status!r}",
        flush=True,
    )
    return key_values, original_status, test_status


def _sql_literal(value: object) -> str:
    """Render a scalar business-key value as a safe Snowflake SQL literal."""
    if value is None:
        raise ValueError("Business-key values must not be null.")
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _sql_identifier(value: str, label: str) -> str:
    """Validate a simple SQL identifier before interpolating it into SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def _run_controlled_scd2_change(
    *,
    snapshot_path: Path,
    source: SourceConfig,
    credentials: str,
    source_run_id: str,
    workflow_run_id: str,
    attempt: str,
) -> None:
    """Verify a tracked STATUS change creates one new current SCD2 version."""
    for label, value in (
        ("source run ID", source_run_id),
        ("workflow run ID", workflow_run_id),
        ("workflow attempt", attempt),
    ):
        if not re.fullmatch(r"[0-9]+", value):
            raise ValueError(f"{label} must contain digits only.")

    test_dataset = f"SNOWFLAKE_SCD2_TEST_{source_run_id}_{workflow_run_id}_{attempt}"
    if not re.fullmatch(r"SNOWFLAKE_SCD2_TEST_[0-9]+_[0-9]+_[0-9]+", test_dataset):
        raise ValueError("Unexpected controlled-test schema name.")

    pipeline_name = f"toronto_active_scd2_change_{workflow_run_id}_{attempt}"
    probe = dlt.pipeline(
        pipeline_name=f"toronto_scd2_probe_{workflow_run_id}_{attempt}",
        destination=dlt.destinations.snowflake(
            credentials=credentials, use_timestamp_tz=False
        ),
        dataset_name=test_dataset,
        dev_mode=False,
    )

    try:
        with tempfile.TemporaryDirectory(
            prefix="toronto-snowflake-scd2-change-"
        ) as temp:
            original_path = Path(temp) / "active_permits_original.parquet"
            changed_path = Path(temp) / "active_permits_changed.parquet"
            key_values, original_status, test_status = (
                _prepare_changed_active_snapshots(
                    snapshot_path=snapshot_path,
                    original_path=original_path,
                    changed_path=changed_path,
                    source=source,
                    workflow_run_id=workflow_run_id,
                    attempt=attempt,
                )
            )

            load_scd2(
                credentials=credentials,
                snapshot_path=original_path,
                source=source,
                dataset_name=test_dataset,
                destination_kind="snowflake",
                pipeline_name=pipeline_name,
            )
            before = _count_rows(probe, source.warehouse_table)
            if before != (1, 1, 0):
                raise ValueError(
                    "Controlled-test seed did not create exactly one current row: "
                    f"{before!r}"
                )

            load_scd2(
                credentials=credentials,
                snapshot_path=changed_path,
                source=source,
                dataset_name=test_dataset,
                destination_kind="snowflake",
                pipeline_name=pipeline_name,
            )
            after = _count_rows(probe, source.warehouse_table)
            if after != (2, 1, 1):
                raise ValueError(
                    "Controlled SCD2 change produced unexpected table counts: "
                    f"before={before!r}, after={after!r}, expected={(2, 1, 1)!r}"
                )

            table_name = _sql_identifier(source.warehouse_table, "warehouse table")
            key_columns = [
                _sql_identifier(column, "primary-key column")
                for column in source.primary_key
            ]
            where = " AND ".join(
                f"{column} = {_sql_literal(value)}"
                for column, value in zip(key_columns, key_values, strict=True)
            )
            with probe.sql_client() as client:
                versions = client.execute_sql(
                    f"SELECT STATUS, _DLT_VALID_TO FROM {table_name} WHERE {where}"
                )

            expired = [row for row in versions if row[1] is not None]
            current = [row for row in versions if row[1] is None]
            if (
                len(versions) != 2
                or len(expired) != 1
                or len(current) != 1
                or str(expired[0][0]) != original_status
                or str(current[0][0]) != test_status
            ):
                raise ValueError(
                    "Controlled SCD2 version transition failed: "
                    f"versions={versions!r}, original_status={original_status!r}, "
                    f"test_status={test_status!r}"
                )

            print(
                "PASS active_permits: one tracked STATUS change created exactly "
                "one new current version and expired the previous version.",
                flush=True,
            )
    finally:
        with probe.sql_client() as client:
            client.execute_sql(f"DROP SCHEMA IF EXISTS TORONTO.{test_dataset} CASCADE")
        print(
            f"Cleaned up temporary Snowflake schema TORONTO.{test_dataset}.",
            flush=True,
        )


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

        _run_controlled_scd2_change(
            snapshot_path=Path(temp) / "active_permits" / "data.parquet",
            source=sources["active_permits"],
            credentials=app.warehouse_credentials,
            source_run_id=os.environ["SNAPSHOT_SOURCE_RUN_ID"],
            workflow_run_id=os.environ["GITHUB_RUN_ID"],
            attempt=os.environ["GITHUB_RUN_ATTEMPT"],
        )

    print(
        f"SNOWFLAKE SNAPSHOT BASELINE PASSED: source run {bundle['source_run_id']} "
        f"loaded into TORONTO.{dataset}; unchanged replay and controlled SCD2 "
        "change test passed.",
        flush=True,
    )


if __name__ == "__main__":
    main()
