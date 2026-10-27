"""Refresh canonical Snowflake RAW from a recent, validated CD snapshot."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from zipfile import ZipFile

import snowflake.connector
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import SourceConfig, load_config
from toronto_housing_ingestion.dlt_loader import load_scd2

SOURCE_NAMES = ("active_permits", "cleared_permits")
EXPECTED_MEMBERS = {
    "bundle.json",
    "active_permits/data.parquet",
    "active_permits/manifest.json",
    "cleared_permits/data.parquet",
    "cleared_permits/manifest.json",
}
EXPECTED_TABLES = {
    "active_permits": "RAW_ACTIVE_PERMITS",
    "cleared_permits": "RAW_CLEARED_PERMITS",
}
PIPELINE_NAMES = {
    "active_permits": "toronto_active_permits_scd2",
    "cleared_permits": "toronto_cleared_permits_scd2",
}
MAX_SNAPSHOT_AGE = timedelta(hours=36)
MAX_METADATA_AGE_SECONDS = 36 * 60 * 60


def _read_json(value: bytes, label: str) -> dict:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return parsed


def _timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone.")
    return parsed.astimezone(UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_and_extract_bundle(
    bundle_path: Path,
    destination: Path,
    sources: dict[str, SourceConfig],
    expected_run_id: str,
    expected_commit: str,
) -> dict:
    with ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
        if names != EXPECTED_MEMBERS:
            raise ValueError(
                "Snapshot bundle members differ from the expected five files. "
                f"Missing={sorted(EXPECTED_MEMBERS - names)}, "
                f"extra={sorted(names - EXPECTED_MEMBERS)}"
            )
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"Snapshot bundle failed ZIP integrity: {bad_member}")
        bundle = _read_json(archive.read("bundle.json"), "bundle.json")
        if bundle.get("format_version") != 1:
            raise ValueError("Unsupported snapshot bundle format version.")
        if str(bundle.get("source_run_id")) != expected_run_id:
            raise ValueError("Bundle run ID does not match the selected CD run.")
        if bundle.get("source_commit") != expected_commit:
            raise ValueError("Bundle commit does not match the selected CD run.")

        started_at = _timestamp(
            bundle.get("extraction_started_at_utc", ""),
            "bundle extraction_started_at_utc",
        )
        age = datetime.now(UTC) - started_at
        if age < timedelta(minutes=-5):
            raise ValueError(
                "Snapshot extraction timestamp is unexpectedly in the future."
            )
        if age > MAX_SNAPSHOT_AGE:
            raise ValueError(
                "Snapshot is older than 36 hours; select a recent successful CD run "
                "rather than refreshing canonical RAW from stale data."
            )

        summaries = bundle.get("sources")
        if not isinstance(summaries, dict):
            raise ValueError("Bundle sources metadata must be a JSON object.")
        destination.mkdir(parents=True, exist_ok=True)

        for name in SOURCE_NAMES:
            source = sources.get(name)
            summary = summaries.get(name)
            if source is None or not isinstance(summary, dict):
                raise ValueError(f"Bundle or configuration is missing {name}.")
            snapshot_member = f"{name}/data.parquet"
            manifest_member = f"{name}/manifest.json"
            source_dir = destination / name
            source_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = source_dir / "data.parquet"
            manifest_path = source_dir / "manifest.json"
            with (
                archive.open(snapshot_member) as source_stream,
                snapshot_path.open("wb") as destination_stream,
            ):
                shutil.copyfileobj(source_stream, destination_stream)
            manifest_path.write_bytes(archive.read(manifest_member))

            manifest = _read_json(manifest_path.read_bytes(), manifest_member)
            digest = _sha256(snapshot_path)
            if digest != summary.get("sha256") or digest != manifest.get("sha256"):
                raise ValueError(f"{name}: snapshot SHA-256 verification failed.")

            parquet = pq.ParquetFile(snapshot_path)
            row_count = int(parquet.metadata.num_rows)
            columns = list(parquet.schema_arrow.names)
            if (
                row_count < 1
                or row_count != summary.get("row_count")
                or row_count != manifest.get("row_count")
            ):
                raise ValueError(f"{name}: snapshot row counts disagree or are empty.")
            if sorted(columns) != sorted(summary.get("columns", [])):
                raise ValueError(
                    f"{name}: snapshot columns disagree with bundle metadata."
                )
            if sorted(columns) != sorted(manifest.get("columns", [])):
                raise ValueError(
                    f"{name}: snapshot columns disagree with its manifest."
                )
            missing = sorted(
                required
                for required in source.required_columns
                if required.lower() not in {column.lower() for column in columns}
            )
            if missing:
                raise ValueError(
                    f"{name}: snapshot is missing required columns: {missing}"
                )
            if (
                manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "loaded"
            ):
                raise ValueError(
                    f"{name}: manifest does not record a successful warehouse load."
                )
            if manifest.get("warehouse_since") != source.warehouse_since:
                raise ValueError(
                    f"{name}: warehouse date cutoff differs from configuration."
                )
            if manifest.get("warehouse_date_column") != source.warehouse_date_column:
                raise ValueError(
                    f"{name}: warehouse date column differs from configuration."
                )
            extracted_at = _timestamp(
                manifest.get("extracted_at_utc", ""), f"{name} extracted_at_utc"
            )
            if extracted_at < started_at:
                raise ValueError(
                    f"{name}: snapshot predates this CD extraction marker."
                )
            print(f"Validated {name}: {row_count:,} rows, sha256={digest}")

    return bundle


def _snowflake_connection_options(credentials: str, key_path: Path) -> dict[str, str]:
    parts = urlsplit(credentials)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    if "private_key" not in params:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION has no private_key parameter.")
    try:
        key_bytes = base64.b64decode(params["private_key"][0], validate=True)
    except ValueError as exc:
        raise ValueError("Snowflake private_key is not valid base64.") from exc
    if not key_bytes:
        raise ValueError("Snowflake private_key decoded to an empty value.")
    key_path.write_bytes(key_bytes)
    key_path.chmod(0o600)
    return {
        "account": parts.hostname,
        "user": unquote(parts.username),
        "private_key_file": str(key_path),
        "role": "TORONTO_INGEST_ROLE",
        "warehouse": "TORONTO_XS_WH",
        "database": "TORONTO",
        "schema": "RAW",
    }


def _watermark(cursor, pipeline_name: str):
    cursor.execute(
        "SELECT MAX(inserted_at) "
        "FROM TORONTO.RAW._DLT_LOADS "
        "WHERE schema_name = %s AND status = 0",
        (pipeline_name,),
    )
    return cursor.fetchone()[0]


def _connect_and_preflight(connection_options: dict) -> dict[str, datetime]:
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        role, database, schema = cursor.fetchone()
        if str(role).upper() != "TORONTO_INGEST_ROLE":
            raise ValueError(f"Unexpected Snowflake role: {role!r}")
        if str(database).upper() != "TORONTO" or str(schema).upper() != "RAW":
            raise ValueError(
                f"Unexpected Snowflake session context: database={database!r}, schema={schema!r}"
            )

        cursor.execute(
            "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'RAW'"
        )
        available = {str(row[0]).upper() for row in cursor.fetchall()}
        required = {*EXPECTED_TABLES.values(), "_DLT_LOADS"}
        missing = required - available
        if missing:
            raise ValueError(
                f"TORONTO.RAW is missing required existing tables: {sorted(missing)}"
            )

        watermarks: dict[str, datetime] = {}
        for name in SOURCE_NAMES:
            table = EXPECTED_TABLES[name]
            cursor.execute(
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) FROM TORONTO.RAW.{table}"
            )
            total, current, expired = cursor.fetchone()
            if (
                not total
                or not current
                or int(total) != int(current) + int(expired or 0)
            ):
                raise ValueError(
                    f"TORONTO.RAW.{table} preflight counts do not reconcile: "
                    f"total={total}, current={current}, expired={expired}"
                )
            watermark = _watermark(cursor, PIPELINE_NAMES[name])
            if watermark is None:
                raise ValueError(
                    f"No successful prior dlt load metadata found for {PIPELINE_NAMES[name]}"
                )
            watermarks[name] = watermark
            print(
                f"Preflight TORONTO.RAW.{table}: total={int(total):,}, "
                f"current={int(current):,}, expired={int(expired or 0):,}; "
                f"last successful load={watermark}"
            )
        return watermarks
    finally:
        cursor.close()
        connection.close()


def _verify_refresh(
    connection_options: dict, old_watermarks: dict[str, datetime]
) -> None:
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        for name in SOURCE_NAMES:
            table = EXPECTED_TABLES[name]
            cursor.execute(
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) FROM TORONTO.RAW.{table}"
            )
            total, current, expired = cursor.fetchone()
            if (
                not total
                or not current
                or int(total) != int(current) + int(expired or 0)
            ):
                raise ValueError(
                    f"TORONTO.RAW.{table} post-load SCD2 counts do not reconcile: "
                    f"total={total}, current={current}, expired={expired}"
                )

            cursor.execute(
                "SELECT MAX(inserted_at), "
                "DATEDIFF('second', MAX(inserted_at), CURRENT_TIMESTAMP()) "
                "FROM TORONTO.RAW._DLT_LOADS "
                "WHERE schema_name = %s AND status = 0",
                (PIPELINE_NAMES[name],),
            )
            latest_load, age_seconds = cursor.fetchone()
            if latest_load is None or latest_load <= old_watermarks[name]:
                raise ValueError(
                    f"{PIPELINE_NAMES[name]} did not record a newer successful load."
                )
            if age_seconds is None or int(age_seconds) < 0:
                raise ValueError(
                    f"Invalid load-metadata age for {PIPELINE_NAMES[name]}."
                )
            if int(age_seconds) > MAX_METADATA_AGE_SECONDS:
                raise ValueError(
                    f"Load metadata for {PIPELINE_NAMES[name]} is still stale: "
                    f"age={int(age_seconds)} seconds."
                )
            print(
                f"Verified TORONTO.RAW.{table}: total={int(total):,}, "
                f"current={int(current):,}, expired={int(expired or 0):,}; "
                f"new successful load metadata age={int(age_seconds)} seconds"
            )
    finally:
        cursor.close()
        connection.close()


def main() -> None:
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    if not credentials:
        raise ValueError("TORONTO_WAREHOUSE_CREDENTIALS is required.")
    if os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower() != "snowflake":
        raise ValueError("TORONTO_WAREHOUSE_KIND must be snowflake.")
    if os.environ.get("TORONTO_WAREHOUSE_DATASET", "").upper() != "RAW":
        raise ValueError("Refusing to load outside canonical dataset TORONTO.RAW.")

    run_id = os.environ.get("SNAPSHOT_SOURCE_RUN_ID", "")
    commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
    if not re.fullmatch(r"[0-9]+", run_id):
        raise ValueError("SNAPSHOT_SOURCE_RUN_ID must contain digits only.")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("SNAPSHOT_SOURCE_COMMIT must be a full commit SHA.")

    app = load_config(Path("open_data_config.yml"))
    sources = {source.name: source for source in app.sources}
    for name in SOURCE_NAMES:
        source = sources.get(name)
        if (
            source is None
            or not source.warehouse_load
            or source.warehouse_strategy != "scd2"
            or source.warehouse_table.upper() != EXPECTED_TABLES[name]
        ):
            raise ValueError(
                f"{name}: configuration differs from expected canonical SCD2 target."
            )

    bundle_path = Path(os.environ.get("SNAPSHOT_BUNDLE_PATH", ""))
    if not bundle_path.is_file() or bundle_path.stat().st_size == 0:
        raise FileNotFoundError(f"Snapshot bundle is missing or empty: {bundle_path}")

    with tempfile.TemporaryDirectory(prefix="toronto-snowflake-raw-refresh-") as temp:
        root = Path(temp)
        extracted = root / "snapshot"
        _validate_and_extract_bundle(bundle_path, extracted, sources, run_id, commit)
        key_path = root / "snowflake_key.p8"
        connection_options = _snowflake_connection_options(credentials, key_path)
        old_watermarks = _connect_and_preflight(connection_options)

        for name in SOURCE_NAMES:
            result = load_scd2(
                credentials=credentials,
                snapshot_path=extracted / name / "data.parquet",
                source=sources[name],
                dataset_name="RAW",
                destination_kind="snowflake",
                pipeline_name=PIPELINE_NAMES[name],
            )
            print(
                f"Loaded {name} into TORONTO.RAW.{result.table_name} "
                "using the validated CD snapshot."
            )

        _verify_refresh(connection_options, old_watermarks)

    print(
        f"SNOWFLAKE CANONICAL RAW REFRESH PASSED: source CD run {run_id}; "
        "both SCD2 tables and successful load metadata verified."
    )


if __name__ == "__main__":
    main()
