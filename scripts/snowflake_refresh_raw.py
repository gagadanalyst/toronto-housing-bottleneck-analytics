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
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit
from zipfile import ZipFile

import snowflake.connector
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import SourceConfig, load_config
from toronto_housing_ingestion.dlt_loader import load_scd2
from toronto_housing_ingestion.snowflake_ledger import (
    ensure_snapshot_ledger_quarantine_columns,
)

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
SNAPSHOT_LEDGER_TABLE = "TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"


def _validated_dataset_schema(value: str) -> str:
    """Accept only the run-scoped RAW staging schema, never production RAW."""
    normalized = str(value).upper()
    if not re.fullmatch(r"RAW_STAGE_[0-9]+_[0-9]+", normalized):
        raise ValueError(
            "Snowflake archive loading is restricted to a run-scoped "
            "TORONTO.RAW_STAGE_<run_id>_<attempt> candidate schema."
        )
    return normalized


def _snapshot_ledger_table(dataset: str) -> str:
    schema = _validated_dataset_schema(dataset)
    return f"TORONTO.{schema}.TORONTO_SNAPSHOT_LEDGER"


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
        if bundle.get("snapshot_stage") != "raw":
            raise ValueError(
                "Canonical Snowflake refresh requires a raw snapshot bundle."
            )
        if bundle.get("complete") is not True:
            raise ValueError("Raw snapshot bundle is incomplete.")
        if bundle.get("approved_for_warehouse") is not True:
            raise ValueError(
                "Raw snapshot bundle is not approved for warehouse loading."
            )
        expected_attempt = os.environ.get("SNAPSHOT_SOURCE_RUN_ATTEMPT", "1")
        if str(bundle.get("source_run_attempt")) != expected_attempt:
            raise ValueError("Bundle workflow attempt does not match the selected run.")
        if bundle.get("snapshot_stage") != "raw":
            raise ValueError(
                "Canonical Snowflake refresh requires a raw snapshot bundle."
            )
        if bundle.get("complete") is not True:
            raise ValueError("Raw snapshot bundle is incomplete.")
        if bundle.get("approved_for_warehouse") is not True:
            raise ValueError(
                "Raw snapshot bundle is not approved for warehouse loading."
            )

        expected_attempt = os.environ.get("SNAPSHOT_SOURCE_RUN_ATTEMPT", "")
        if expected_attempt:
            if not expected_attempt.isdigit():
                raise ValueError(
                    "SNAPSHOT_SOURCE_RUN_ATTEMPT must contain digits only."
                )
            if str(bundle.get("source_run_attempt")) != expected_attempt:
                raise ValueError(
                    "Bundle workflow attempt does not match the selected run."
                )
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
        # Immutable archives are intentionally replayable beyond 36 hours.
        # Chronological safety is enforced by the durable processing ledger below.

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
                manifest.get("source_name") != name
                or manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "not_loaded"
                or manifest.get("warehouse_eligible") is not True
                or summary.get("warehouse_eligible") is not True
            ):
                raise ValueError(
                    f"{name}: raw snapshot is not approved and ready for warehouse loading."
                )
            if (
                manifest.get("validation_errors")
                or manifest.get("warehouse_quality_blockers")
                or summary.get("validation_errors")
                or summary.get("warehouse_quality_blockers")
            ):
                raise ValueError(
                    f"{name}: archived manifest/summary contains quality or validation blockers."
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
            summary_extracted_at = _timestamp(
                summary.get("extracted_at_utc", ""),
                f"{name} bundle summary extracted_at_utc",
            )
            if extracted_at != summary_extracted_at:
                raise ValueError(
                    f"{name}: manifest and bundle extraction timestamps disagree."
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
    # Query-string parsing treats an unescaped '+' as a space. Base64 uses '+',
    # so restore it before strict decoding; a Base64 value cannot contain spaces.
    encoded_key = params["private_key"][0].replace(" ", "+")
    try:
        key_bytes = base64.b64decode(encoded_key, validate=True)
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


def _watermark(cursor, pipeline_name: str, dataset: str) -> datetime | None:
    schema = _validated_dataset_schema(dataset)
    cursor.execute(
        f'SELECT MAX(inserted_at) FROM TORONTO."{schema}"._DLT_LOADS '
        "WHERE schema_name = %s AND status = 0",
        (pipeline_name,),
    )
    return cursor.fetchone()[0]


def _connect_and_preflight(
    connection_options: dict, dataset: str
) -> dict[str, datetime]:
    schema_name = _validated_dataset_schema(dataset)
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        role, database, schema = cursor.fetchone()
        if str(role).upper() != "TORONTO_INGEST_ROLE":
            raise ValueError(f"Unexpected Snowflake role: {role!r}")
        if str(database).upper() != "TORONTO" or str(schema).upper() != schema_name:
            raise ValueError(
                f"Unexpected Snowflake session context: database={database!r}, "
                f"schema={schema!r}; expected TORONTO.{schema_name}."
            )

        cursor.execute(
            "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s",
            (schema_name,),
        )
        available = {str(row[0]).upper() for row in cursor.fetchall()}
        required = {
            *EXPECTED_TABLES.values(),
            "_DLT_LOADS",
            "TORONTO_SNAPSHOT_LEDGER",
            "_DLT_PIPELINE_STATE",
        }
        missing = required - available
        if missing:
            raise ValueError(
                f"TORONTO.{schema_name} is missing required cloned tables: {sorted(missing)}"
            )

        watermarks: dict[str, datetime] = {}
        for name in SOURCE_NAMES:
            table = EXPECTED_TABLES[name]
            cursor.execute(
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) "
                f'FROM TORONTO."{schema_name}".{table}'
            )
            total, current, expired = cursor.fetchone()
            if (
                not total
                or not current
                or int(total) != int(current) + int(expired or 0)
            ):
                raise ValueError(
                    f"TORONTO.{schema_name}.{table} preflight counts do not reconcile: "
                    f"total={total}, current={current}, expired={expired}"
                )
            watermark = _watermark(cursor, PIPELINE_NAMES[name], schema_name)
            if watermark is None:
                raise ValueError(
                    f"No successful prior dlt load metadata found for {PIPELINE_NAMES[name]}"
                )
            watermarks[name] = watermark
            print(
                f"Preflight TORONTO.{schema_name}.{table}: total={int(total):,}, "
                f"current={int(current):,}, expired={int(expired or 0):,}; "
                f"last successful load={watermark}"
            )
        return watermarks
    finally:
        cursor.close()
        connection.close()


def _verify_refresh(
    connection_options: dict,
    old_watermarks: dict[str, datetime],
    expected_sources: tuple[str, ...] = SOURCE_NAMES,
    dataset: str = "RAW_STAGE_0_0",
) -> None:
    schema_name = _validated_dataset_schema(dataset)
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        for name in expected_sources:
            table = EXPECTED_TABLES[name]
            cursor.execute(
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) "
                f'FROM TORONTO."{schema_name}".{table}'
            )
            total, current, expired = cursor.fetchone()
            if (
                not total
                or not current
                or int(total) != int(current) + int(expired or 0)
            ):
                raise ValueError(
                    f"TORONTO.{schema_name}.{table} post-load SCD2 counts do not "
                    f"reconcile: total={total}, current={current}, expired={expired}"
                )
            cursor.execute(
                f'''SELECT MAX(inserted_at),
                           DATEDIFF('second', MAX(inserted_at), CURRENT_TIMESTAMP())
                    FROM TORONTO."{schema_name}"._DLT_LOADS
                    WHERE schema_name = %s AND status = 0''',
                (PIPELINE_NAMES[name],),
            )
            latest_load, age_seconds = cursor.fetchone()
            if latest_load is None or latest_load <= old_watermarks[name]:
                raise ValueError(
                    f"{PIPELINE_NAMES[name]} did not record a newer successful load."
                )
            if (
                age_seconds is None
                or int(age_seconds) < 0
                or int(age_seconds) > MAX_METADATA_AGE_SECONDS
            ):
                raise ValueError(
                    f"Invalid/stale load metadata for {PIPELINE_NAMES[name]}: {age_seconds}"
                )
            print(
                f"Verified TORONTO.{schema_name}.{table}: total={int(total):,}, "
                f"current={int(current):,}, expired={int(expired or 0):,}; "
                f"new load metadata age={int(age_seconds)} seconds"
            )
    finally:
        cursor.close()
        connection.close()


def _snapshot_order(extracted_at, run_id: str, attempt: str | int):
    """Return a deterministic chronological key for a snapshot identity."""
    if not str(run_id).isdigit() or not str(attempt).isdigit():
        raise ValueError("Snapshot run ID and attempt must contain digits only.")
    if isinstance(extracted_at, datetime):
        parsed = extracted_at
        if parsed.tzinfo is None:
            raise ValueError("Snapshot timestamps must include a timezone.")
        parsed = parsed.astimezone(UTC)
    else:
        parsed = _timestamp(str(extracted_at), "snapshot extraction timestamp")
    return parsed, int(run_id), int(attempt)


def _validated_snapshot_ledger_table(value: str) -> str:
    # Restrict the override to a fully qualified ledger in the TORONTO database.
    if not re.fullmatch(
        r"TORONTO\.[A-Z_][A-Z0-9_]*\.TORONTO_SNAPSHOT_LEDGER",
        value,
        flags=re.IGNORECASE,
    ):
        raise ValueError(f"Unsafe Snowflake snapshot ledger table: {value!r}")
    return value.upper()


def _prepare_snapshot_ledger(
    connection_options: dict,
    *,
    bundle: dict,
    run_id: str,
    attempt: str,
    commit: str,
    release_tag: str,
    asset_name: str,
    ledger_table: str = SNAPSHOT_LEDGER_TABLE,
) -> dict[str, str]:
    """Create the durable per-source ledger and enforce chronological replay."""
    started = _timestamp(
        bundle.get("extraction_started_at_utc", ""),
        "bundle extraction_started_at_utc",
    )
    current_order = _snapshot_order(started, run_id, attempt)
    snapshot_id = f"{run_id}:{attempt}"
    ledger_table = _validated_snapshot_ledger_table(ledger_table)
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        try:
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {ledger_table} (
                    SNAPSHOT_ID VARCHAR NOT NULL,
                    SOURCE_RUN_ID VARCHAR NOT NULL,
                    SOURCE_RUN_ATTEMPT NUMBER(10,0) NOT NULL,
                    EXTRACTION_STARTED_AT_UTC TIMESTAMP_TZ NOT NULL,
                    SOURCE_NAME VARCHAR NOT NULL,
                    SOURCE_COMMIT VARCHAR NOT NULL,
                    RELEASE_TAG VARCHAR NOT NULL,
                    ASSET_NAME VARCHAR NOT NULL,
                    STATUS VARCHAR NOT NULL,
                    ERROR VARCHAR,
                    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )"""
            )
            ensure_snapshot_ledger_quarantine_columns(
                cursor, ledger_table.split(".")[1]
            )
        except Exception as exc:
            raise RuntimeError(
                f"Cannot create or initialize {ledger_table}: {exc}"
            ) from exc

        cursor.execute(
            f"""SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT,
                       EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, STATUS,
                       SOURCE_COMMIT, RELEASE_TAG, ASSET_NAME
                FROM {ledger_table}"""
        )
        rows = cursor.fetchall()
        current_statuses = {}
        for row in rows:
            old_id, old_run, old_attempt, old_started, source_name, status = row[:6]
            if old_id != snapshot_id:
                continue
            source_key = str(source_name)
            if source_key not in SOURCE_NAMES or source_key in current_statuses:
                raise RuntimeError(
                    f"Snowflake ledger has an unexpected or duplicate source for {snapshot_id}: {source_key!r}."
                )
            old_timestamp = (
                old_started.astimezone(UTC)
                if isinstance(old_started, datetime)
                else _timestamp(str(old_started), "ledger extraction timestamp")
            )
            if (
                str(old_run) != run_id
                or int(old_attempt) != int(attempt)
                or old_timestamp != started.astimezone(UTC)
                or (len(row) > 6 and row[6] is not None and str(row[6]) != commit)
                or (len(row) > 7 and row[7] is not None and str(row[7]) != release_tag)
                or (len(row) > 8 and row[8] is not None and str(row[8]) != asset_name)
            ):
                raise RuntimeError(
                    f"Snowflake ledger identity conflicts with immutable archive {snapshot_id}/{source_key}."
                )
            normalized_status = str(status).lower()
            if normalized_status not in {"pending", "loading", "succeeded", "failed"}:
                raise RuntimeError(
                    f"Snowflake ledger has unsupported status {status!r} for {snapshot_id}/{source_key}."
                )
            current_statuses[source_key] = normalized_status
        if set(current_statuses) == set(SOURCE_NAMES) and all(
            status == "succeeded" for status in current_statuses.values()
        ):
            return current_statuses

        grouped = {}
        for row in rows:
            old_id, old_run, old_attempt, old_started, old_source, old_status = row[:6]
            if old_id == snapshot_id:
                continue
            entry = grouped.setdefault(
                str(old_id),
                {
                    "order": _snapshot_order(
                        old_started, str(old_run), str(old_attempt)
                    ),
                    "statuses": {},
                },
            )
            entry["statuses"][str(old_source)] = str(old_status).lower()

        earlier_incomplete = []
        later_started = []
        for old_id, entry in grouped.items():
            statuses = entry["statuses"]
            old_order = entry["order"]
            if old_order < current_order:
                missing = sorted(set(SOURCE_NAMES) - statuses.keys())
                if missing or any(
                    statuses.get(name) != "succeeded" for name in SOURCE_NAMES
                ):
                    earlier_incomplete.append((old_id, statuses, missing))
            elif old_order > current_order:
                later_started.extend(
                    (old_id, name, status)
                    for name, status in statuses.items()
                    if status != "pending"
                )

        if earlier_incomplete:
            raise RuntimeError(
                "Snowflake replay blocked by earlier archived snapshots that do not "
                "have both source loads recorded as succeeded: "
                f"{earlier_incomplete}"
            )
        if later_started:
            raise RuntimeError(
                "Out-of-order Snowflake replay rejected because a newer snapshot has "
                "a non-pending source status (its warehouse write may have committed): "
                f"{later_started}"
            )

        for name in SOURCE_NAMES:
            cursor.execute(
                f"""MERGE INTO {ledger_table} AS target
                    USING (
                        SELECT %s AS SNAPSHOT_ID,
                               %s AS SOURCE_RUN_ID,
                               %s::NUMBER(10,0) AS SOURCE_RUN_ATTEMPT,
                               TO_TIMESTAMP_TZ(%s) AS EXTRACTION_STARTED_AT_UTC,
                               %s AS SOURCE_NAME,
                               %s AS SOURCE_COMMIT,
                               %s AS RELEASE_TAG,
                               %s AS ASSET_NAME
                    ) AS incoming
                    ON target.SNAPSHOT_ID = incoming.SNAPSHOT_ID
                       AND target.SOURCE_NAME = incoming.SOURCE_NAME
                    WHEN MATCHED THEN UPDATE SET
                        SOURCE_RUN_ID = incoming.SOURCE_RUN_ID,
                        SOURCE_RUN_ATTEMPT = incoming.SOURCE_RUN_ATTEMPT,
                        EXTRACTION_STARTED_AT_UTC = incoming.EXTRACTION_STARTED_AT_UTC,
                        SOURCE_COMMIT = incoming.SOURCE_COMMIT,
                        RELEASE_TAG = incoming.RELEASE_TAG,
                        ASSET_NAME = incoming.ASSET_NAME,
                        STATUS = IFF(LOWER(target.STATUS) = 'succeeded', target.STATUS, 'pending'),
                        ERROR = IFF(LOWER(target.STATUS) = 'succeeded', target.ERROR, NULL),
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT,
                        EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, SOURCE_COMMIT,
                        RELEASE_TAG, ASSET_NAME, STATUS, ERROR
                    ) VALUES (
                        incoming.SNAPSHOT_ID, incoming.SOURCE_RUN_ID,
                        incoming.SOURCE_RUN_ATTEMPT, incoming.EXTRACTION_STARTED_AT_UTC,
                        incoming.SOURCE_NAME, incoming.SOURCE_COMMIT,
                        incoming.RELEASE_TAG, incoming.ASSET_NAME, 'pending', NULL
                    )""",
                (
                    snapshot_id,
                    run_id,
                    int(attempt),
                    started.isoformat(),
                    name,
                    commit,
                    release_tag,
                    asset_name,
                ),
            )

        cursor.execute(
            f"SELECT SOURCE_NAME, STATUS FROM {ledger_table} WHERE SNAPSHOT_ID = %s",
            (snapshot_id,),
        )
        statuses = {
            str(name): str(status).lower() for name, status in cursor.fetchall()
        }
        missing = sorted(set(SOURCE_NAMES) - statuses.keys())
        if missing:
            raise RuntimeError(
                f"Snowflake snapshot ledger did not register every source: {missing}"
            )
        return statuses
    finally:
        cursor.close()
        connection.close()


def _completed_snapshot_ids(
    connection_options: dict, ledger_table: str = SNAPSHOT_LEDGER_TABLE
) -> set[str]:
    """Return only snapshots with both source loads durably marked succeeded."""
    ledger_table = _validated_snapshot_ledger_table(ledger_table)
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        try:
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {ledger_table} (
                SNAPSHOT_ID VARCHAR NOT NULL,
                SOURCE_RUN_ID VARCHAR NOT NULL,
                SOURCE_RUN_ATTEMPT NUMBER(10,0) NOT NULL,
                EXTRACTION_STARTED_AT_UTC TIMESTAMP_TZ NOT NULL,
                SOURCE_NAME VARCHAR NOT NULL,
                SOURCE_COMMIT VARCHAR NOT NULL,
                RELEASE_TAG VARCHAR NOT NULL,
                ASSET_NAME VARCHAR NOT NULL,
                STATUS VARCHAR NOT NULL,
                ERROR VARCHAR,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )"""
            )
        except Exception as exc:
            raise RuntimeError(
                f"Cannot create or access {ledger_table}. "
                "TORONTO_INGEST_ROLE needs USAGE on database TORONTO and "
                "schema RAW, plus CREATE TABLE on schema TORONTO.RAW."
            ) from exc
        cursor.execute(f"SELECT SNAPSHOT_ID, SOURCE_NAME, STATUS FROM {ledger_table}")
        grouped: dict[str, dict[str, str]] = {}
        for snapshot_id, source_name, status in cursor.fetchall():
            grouped.setdefault(str(snapshot_id), {})[str(source_name)] = str(
                status
            ).lower()
        return {
            snapshot_id
            for snapshot_id, statuses in grouped.items()
            if set(statuses) == set(SOURCE_NAMES)
            and all(statuses[name] == "succeeded" for name in SOURCE_NAMES)
        }
    finally:
        cursor.close()
        connection.close()


def _set_snapshot_source_status(
    connection_options: dict,
    *,
    snapshot_id: str,
    source_name: str,
    status: str,
    error: str | None = None,
    ledger_table: str = SNAPSHOT_LEDGER_TABLE,
    quarantined_row_count: int = 0,
    quarantined_bad_row_count: int = 0,
    quarantined_business_key_count: int = 0,
    quarantine_sha256: str | None = None,
) -> None:
    """Update and verify one per-source ledger status."""
    allowed = {"pending", "loading", "succeeded", "failed"}
    if status not in allowed:
        raise ValueError(f"Unsupported snapshot ledger status: {status!r}")
    ledger_table = _validated_snapshot_ledger_table(ledger_table)
    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""UPDATE {ledger_table}
                SET STATUS = %s, ERROR = %s, UPDATED_AT = CURRENT_TIMESTAMP(),
                    QUARANTINED_ROW_COUNT = %s,
                    QUARANTINED_BAD_ROW_COUNT = %s,
                    QUARANTINED_BUSINESS_KEY_COUNT = %s,
                    QUARANTINE_SHA256 = %s
                WHERE SNAPSHOT_ID = %s AND SOURCE_NAME = %s""",
            (
                status,
                error,
                quarantined_row_count,
                quarantined_bad_row_count,
                quarantined_business_key_count,
                quarantine_sha256,
                snapshot_id,
                source_name,
            ),
        )
        cursor.execute(
            f"SELECT STATUS FROM {ledger_table} "
            "WHERE SNAPSHOT_ID = %s AND SOURCE_NAME = %s",
            (snapshot_id, source_name),
        )
        row = cursor.fetchone()
        if not row or str(row[0]).lower() != status:
            raise RuntimeError(
                f"Could not verify ledger status {status!r} for "
                f"{snapshot_id}/{source_name}."
            )
    finally:
        cursor.close()
        connection.close()


def _assert_canonical_replay_baseline(connection_options: dict) -> None:
    """Require an explicitly identified, complete promoted Snowflake baseline."""
    if os.environ.get("TORONTO_CANONICAL_REPLAY_ENABLED", "").lower() != "true":
        raise RuntimeError(
            "Canonical Snowflake replay is disabled until the isolated historical "
            "rebuild, DuckDB/Snowflake parity validation, and controlled promotion "
            "are complete."
        )

    baseline_id = os.environ.get("TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID", "")
    expected_count_text = os.environ.get(
        "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT", ""
    )
    if not re.fullmatch(r"[0-9]+:[0-9]+", baseline_id):
        raise RuntimeError(
            "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID must be run_id:attempt."
        )
    if not expected_count_text.isdigit() or int(expected_count_text) < 1:
        raise RuntimeError(
            "TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT must be a positive integer."
        )
    expected_count = int(expected_count_text)

    connection = snowflake.connector.connect(**connection_options)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'RAW' "
            "AND TABLE_NAME = 'TORONTO_SNAPSHOT_LEDGER'"
        )
        row = cursor.fetchone()
        if not row or int(row[0] or 0) != 1:
            raise RuntimeError(
                "TORONTO.RAW has no promoted snapshot ledger. Do not bootstrap the "
                "canonical ledger from an empty state; rebuild in an isolated schema."
            )
        cursor.execute(
            "SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT, "
            "EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, STATUS "
            "FROM TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

    grouped: dict[str, dict[str, Any]] = {}
    for snapshot_id, run_id, attempt, started, source_name, status in rows:
        if isinstance(started, datetime):
            timestamp = started.astimezone(UTC)
        else:
            timestamp = _timestamp(str(started), "ledger extraction timestamp")
        entry = grouped.setdefault(
            str(snapshot_id),
            {
                "order": (timestamp, int(run_id), int(attempt)),
                "statuses": {},
            },
        )
        row_order = (timestamp, int(run_id), int(attempt))
        if entry["order"] != row_order:
            raise RuntimeError(
                f"Inconsistent identity metadata for Snowflake snapshot {snapshot_id}."
            )
        source_key = str(source_name)
        if source_key in entry["statuses"]:
            raise RuntimeError(
                f"Duplicate Snowflake baseline ledger entry for {snapshot_id}/{source_key}."
            )
        if source_key not in SOURCE_NAMES:
            raise RuntimeError(
                f"Unexpected source name in Snowflake snapshot ledger: {source_key}."
            )
        entry["statuses"][source_key] = str(status).lower()

    baseline = grouped.get(baseline_id)
    if baseline is None:
        raise RuntimeError(
            f"Promoted baseline snapshot {baseline_id} is absent from the Snowflake ledger."
        )
    baseline_order = baseline["order"]
    through_baseline = {
        snapshot_id: entry
        for snapshot_id, entry in grouped.items()
        if entry["order"] <= baseline_order
    }
    completed = {
        snapshot_id
        for snapshot_id, entry in through_baseline.items()
        if set(entry["statuses"]) == set(SOURCE_NAMES)
        and all(entry["statuses"].get(name) == "succeeded" for name in SOURCE_NAMES)
    }
    if len(through_baseline) != expected_count or len(completed) != expected_count:
        raise RuntimeError(
            "Promoted Snowflake baseline ledger coverage does not match the declared "
            f"baseline: expected {expected_count} complete snapshots through "
            f"{baseline_id}, found {len(through_baseline)} ledger snapshots and "
            f"{len(completed)} fully completed snapshots."
        )
    if baseline_id not in completed:
        raise RuntimeError(
            f"Promoted baseline snapshot {baseline_id} is not complete for both sources."
        )


def main() -> None:
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    dataset = _validated_dataset_schema(os.environ.get("TORONTO_WAREHOUSE_DATASET", ""))
    if not credentials:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is missing.")
    if os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower() != "snowflake":
        raise ValueError("TORONTO_WAREHOUSE_KIND must be 'snowflake'.")
    # Direct writes to TORONTO.RAW are forbidden. The candidate schema must be
    # the unique run-scoped clone prepared by prepare_snowflake_raw_candidate.py.

    run_id = os.environ.get("SNAPSHOT_SOURCE_RUN_ID", "")
    attempt = os.environ.get("SNAPSHOT_SOURCE_RUN_ATTEMPT", "1")
    commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
    release_tag = os.environ.get("SNAPSHOT_ARCHIVE_TAG", "")
    asset_name = os.environ.get("SNAPSHOT_ARCHIVE_ASSET", "")
    if not re.fullmatch(r"[0-9]+", run_id) or not re.fullmatch(r"[0-9]+", attempt):
        raise ValueError("Snapshot source run ID and attempt must contain digits only.")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("SNAPSHOT_SOURCE_COMMIT must be a full commit SHA.")
    if not re.fullmatch(r"toronto-raw-snapshot-archive-[0-9]{4}-[0-9]{2}", release_tag):
        raise ValueError(
            "SNAPSHOT_ARCHIVE_TAG must identify a monthly raw archive Release."
        )
    if asset_name != f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip":
        raise ValueError(
            "Archive asset name does not match the selected run and attempt."
        )

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
        raise FileNotFoundError(
            f"Validated raw snapshot archive is missing: {bundle_path}"
        )

    with tempfile.TemporaryDirectory(prefix="toronto-snowflake-raw-refresh-") as temp:
        root = Path(temp)
        extracted = root / "snapshot"
        bundle = _validate_and_extract_bundle(
            bundle_path, extracted, sources, run_id, commit
        )
        bundle_started = _timestamp(
            bundle.get("extraction_started_at_utc", ""),
            "bundle extraction_started_at_utc",
        )
        expected_tag = f"toronto-raw-snapshot-archive-{bundle_started:%Y-%m}"
        if release_tag != expected_tag:
            raise ValueError(
                f"Monthly archive Release does not match bundle UTC month: "
                f"{release_tag!r} != {expected_tag!r}."
            )

        key_path = root / "snowflake_key.p8"
        connection_options = _snowflake_connection_options(credentials, key_path)
        _assert_canonical_replay_baseline(connection_options)
        connection_options["schema"] = dataset
        old_watermarks = _connect_and_preflight(connection_options, dataset)
        ledger_table = _snapshot_ledger_table(dataset)
        statuses = _prepare_snapshot_ledger(
            connection_options,
            bundle=bundle,
            run_id=run_id,
            attempt=attempt,
            commit=commit,
            release_tag=release_tag,
            asset_name=asset_name,
            ledger_table=ledger_table,
        )
        snapshot_id = f"{run_id}:{attempt}"
        failures: list[str] = []

        for name in SOURCE_NAMES:
            if statuses[name] == "succeeded":
                print(
                    f"Skipping {name}: immutable snapshot {snapshot_id} is already loaded."
                )
                continue

            _set_snapshot_source_status(
                connection_options,
                snapshot_id=snapshot_id,
                source_name=name,
                status="loading",
                ledger_table=ledger_table,
            )
            try:
                manifest_path = extracted / name / "manifest.json"
                manifest = _read_json(manifest_path.read_bytes(), manifest_path.name)
                boundary_timestamp = _timestamp(
                    manifest.get("extracted_at_utc", ""),
                    f"{name} extracted_at_utc",
                ).isoformat()
                result = load_scd2(
                    credentials=credentials,
                    snapshot_path=extracted / name / "data.parquet",
                    source=sources[name],
                    dataset_name=dataset,
                    destination_kind="snowflake",
                    pipeline_name=PIPELINE_NAMES[name],
                    boundary_timestamp=boundary_timestamp,
                    snapshot_context={
                        "snapshot_id": snapshot_id,
                        "source_run_id": run_id,
                        "source_run_attempt": int(attempt),
                        "extracted_at_utc": manifest["extracted_at_utc"],
                        "source_commit": commit,
                        "release_tag": release_tag,
                        "asset_name": asset_name,
                        "parquet_sha256": _sha256(extracted / name / "data.parquet"),
                    },
                )
                _verify_refresh(connection_options, old_watermarks, (name,), dataset)
                _set_snapshot_source_status(
                    connection_options,
                    snapshot_id=snapshot_id,
                    source_name=name,
                    status="succeeded",
                    ledger_table=ledger_table,
                    quarantined_row_count=result.quarantined_row_count,
                    quarantined_bad_row_count=result.quarantined_bad_row_count,
                    quarantined_business_key_count=result.quarantined_business_key_count,
                    quarantine_sha256=result.quarantine_sha256,
                )
                print(
                    f"Loaded {name} into TORONTO.{dataset}.{result.table_name} "
                    f"with boundary_timestamp={boundary_timestamp}."
                )
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"[:4000]
                try:
                    _set_snapshot_source_status(
                        connection_options,
                        snapshot_id=snapshot_id,
                        source_name=name,
                        status="failed",
                        error=message,
                        ledger_table=ledger_table,
                    )
                except Exception as ledger_exc:
                    message += f"; ledger update also failed: {type(ledger_exc).__name__}: {ledger_exc}"
                failures.append(f"{name}: {message}")

        if failures:
            raise RuntimeError(
                "Snowflake snapshot was only partially processed; retrying the same "
                "immutable archive resumes unfinished sources. " + "; ".join(failures)
            )

    print(
        f"SNOWFLAKE CANONICAL RAW REFRESH PASSED: run {run_id}, attempt {attempt}; "
        f"both source ledger entries are succeeded in TORONTO.{dataset}."
    )


if __name__ == "__main__":
    main()
