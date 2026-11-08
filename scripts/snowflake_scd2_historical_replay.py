"""Replay validated Toronto permit snapshots into an isolated Snowflake schema."""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from zipfile import ZipFile

import snowflake.connector
from pyarrow import Table
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import SourceConfig, load_config
from toronto_housing_ingestion.dlt_loader import (
    _iter_scd2_snapshot_rows,
    _resolve_scd2_key_columns,
    _validate_scd2_snapshot,
    load_scd2,
)

REPOSITORY = "gagadanalyst/toronto-housing-bottleneck-analytics"
SOURCE_NAMES = ("active_permits", "cleared_permits")
RUN_IDS = (
    "40470002594",
    "40482942648",
    "40491722684",
    "40508242800",
    "40513793300",
    "40533706926",
    "40567655707",
    "40607541510",
    "40622186735",
    "40630228426",
    "40638789933",
    "40641089502",
    "40668146616",
    "40679228717",
    "40788712364",
    "40796993767",
    "40820837442",
    "40848285474",
    "41034120228",
    "41187888283",
    "41295484744",
    "41396374837",
    "41403055348",
    "41440929410",
    "41533746699",
    "41628672679",
    "41824174680",
    "42022028625",
)
EXPECTED_METRICS = {
    "active_permits": {"total": 104_321, "current": 98_511, "expired": 5_810},
    "cleared_permits": {"total": 190_323, "current": 190_274, "expired": 49},
}
EXPECTED_ACCOUNT = "bihaste-tu44125"
EXPECTED_DATABASE = "TORONTO"
EXPECTED_ROLE = "TORONTO_INGEST_ROLE"
EXPECTED_WAREHOUSE = "TORONTO_XS_WH"
TIMESTAMP_ONE = "2026-11-01T12:34:56.123456+00:00"
TIMESTAMP_TWO = "2026-11-02T13:45:56.654321+00:00"
TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6'


@dataclass(frozen=True)
class Snapshot:
    run_id: str
    source_name: str
    path: Path
    extracted_at: str
    row_count: int
    sha256: str
    manifest: dict


def _identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value.upper()


def _timestamp(value: str, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{label} is not a valid ISO-8601 timestamp: {value!r}"
        ) from exc
    if result.tzinfo is None:
        raise ValueError(f"{label} must include a timezone: {value!r}")
    return result.astimezone(UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, label: str) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return value


def _run_metadata(run_id: str) -> dict:
    result = subprocess.run(
        [
            "gh",
            "run",
            "view",
            run_id,
            "--repo",
            REPOSITORY,
            "--json",
            "name,status,conclusion,headBranch,headSha,event",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    expected = {
        "name": "CD",
        "status": "completed",
        "conclusion": "success",
        "headBranch": "main",
        "event": "workflow_run",
    }
    differences = {
        key: (value, metadata.get(key))
        for key, value in expected.items()
        if metadata.get(key) != value
    }
    if differences:
        raise ValueError(
            f"Run {run_id} is not a successful main-branch CD run: {differences}"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", str(metadata.get("headSha", ""))):
        raise ValueError(f"Run {run_id} returned an invalid source commit SHA.")
    return metadata


def _download_bundles(root: Path) -> dict[str, dict]:
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise ValueError("This replay must run in the expected GitHub repository.")
    if not os.environ.get("GH_TOKEN"):
        raise ValueError(
            "GH_TOKEN is required to download historical snapshot artifacts."
        )

    metadata_by_run: dict[str, dict] = {}
    for run_id in RUN_IDS:
        run_dir = root / run_id
        archive_path = run_dir / "toronto-snapshot-bundle.zip"
        run_dir.mkdir(parents=True, exist_ok=True)
        metadata = _run_metadata(run_id)
        if not archive_path.is_file():
            subprocess.run(
                [
                    "gh",
                    "run",
                    "download",
                    run_id,
                    "--repo",
                    REPOSITORY,
                    "--name",
                    f"toronto-snapshot-bundle-{run_id}",
                    "--dir",
                    str(run_dir),
                ],
                check=True,
            )
        if not archive_path.is_file() or archive_path.stat().st_size == 0:
            raise FileNotFoundError(f"Snapshot artifact missing for CD run {run_id}.")
        metadata_by_run[run_id] = metadata
        print(f"Downloaded/verified artifact for CD run {run_id}.", flush=True)
    return metadata_by_run


def _extract_and_validate_bundles(
    bundle_root: Path,
    prepared_root: Path,
    metadata_by_run: dict[str, dict],
    sources: dict[str, SourceConfig],
) -> dict[str, list[Snapshot]]:
    expected_members = {"bundle.json"}
    for name in SOURCE_NAMES:
        expected_members.update({f"{name}/data.parquet", f"{name}/manifest.json"})

    prepared: dict[str, list[Snapshot]] = {name: [] for name in SOURCE_NAMES}
    last_timestamp: dict[str, datetime | None] = {name: None for name in SOURCE_NAMES}

    for run_id in RUN_IDS:
        archive_path = bundle_root / run_id / "toronto-snapshot-bundle.zip"
        run_metadata = metadata_by_run[run_id]
        with ZipFile(archive_path) as archive:
            names = set(archive.namelist())
            if names != expected_members:
                raise ValueError(
                    f"Run {run_id}: unexpected bundle contents; "
                    f"missing={sorted(expected_members - names)}, "
                    f"extra={sorted(names - expected_members)}"
                )
            bad_member = archive.testzip()
            if bad_member is not None:
                raise ValueError(f"Run {run_id}: corrupt ZIP member {bad_member!r}.")

            bundle = json.loads(archive.read("bundle.json"))
            if bundle.get("format_version") != 1:
                raise ValueError(f"Run {run_id}: unsupported bundle format.")
            if str(bundle.get("source_run_id")) != run_id:
                raise ValueError(f"Run {run_id}: bundle source_run_id mismatch.")
            if bundle.get("source_commit") != run_metadata["headSha"]:
                raise ValueError(f"Run {run_id}: bundle source commit mismatch.")

            for source_name in SOURCE_NAMES:
                source = sources[source_name]
                summary = bundle.get("sources", {}).get(source_name)
                if not isinstance(summary, dict):
                    raise ValueError(
                        f"Run {run_id}: missing {source_name} bundle metadata."
                    )

                data_member = f"{source_name}/data.parquet"
                manifest_member = f"{source_name}/manifest.json"
                destination = prepared_root / run_id / source_name
                destination.mkdir(parents=True, exist_ok=True)
                snapshot_path = destination / "data.parquet"
                manifest_path = destination / "manifest.json"
                with (
                    archive.open(data_member) as source_stream,
                    snapshot_path.open("wb") as target_stream,
                ):
                    shutil.copyfileobj(source_stream, target_stream)
                manifest_path.write_bytes(archive.read(manifest_member))

                manifest = _read_json(manifest_path, manifest_member)
                digest = _sha256(snapshot_path)
                if digest != summary.get("sha256") or digest != manifest.get("sha256"):
                    raise ValueError(
                        f"Run {run_id} {source_name}: Parquet SHA-256 mismatch."
                    )
                parquet = pq.ParquetFile(snapshot_path)
                row_count = int(parquet.metadata.num_rows)
                columns = list(parquet.schema_arrow.names)
                if row_count != int(summary.get("row_count", -1)) or row_count != int(
                    manifest.get("row_count", -1)
                ):
                    raise ValueError(f"Run {run_id} {source_name}: row-count mismatch.")
                if sorted(columns) != sorted(summary.get("columns", [])) or sorted(
                    columns
                ) != sorted(manifest.get("columns", [])):
                    raise ValueError(f"Run {run_id} {source_name}: schema mismatch.")
                if (
                    manifest.get("status") != "success"
                    or manifest.get("warehouse_status") != "loaded"
                ):
                    raise ValueError(
                        f"Run {run_id} {source_name}: unsuccessful source manifest."
                    )
                if manifest.get("warehouse_since") != source.warehouse_since:
                    raise ValueError(
                        f"Run {run_id} {source_name}: warehouse_since mismatch."
                    )
                if (
                    manifest.get("warehouse_date_column")
                    != source.warehouse_date_column
                ):
                    raise ValueError(
                        f"Run {run_id} {source_name}: warehouse date column mismatch."
                    )
                if (
                    int(manifest.get("raw_size_bytes", -1))
                    != snapshot_path.stat().st_size
                ):
                    raise ValueError(
                        f"Run {run_id} {source_name}: raw file-size mismatch."
                    )
                required = {column.lower() for column in source.required_columns}
                actual = {column.lower() for column in columns}
                missing = sorted(required - actual)
                if missing:
                    raise ValueError(
                        f"Run {run_id} {source_name}: missing columns {missing}."
                    )

                extracted_at = str(manifest.get("extracted_at_utc", ""))
                extracted_dt = _timestamp(
                    extracted_at, f"{run_id} {source_name} extracted_at_utc"
                )
                if summary.get("extracted_at_utc") != extracted_at:
                    raise ValueError(
                        f"Run {run_id} {source_name}: timestamp metadata mismatch."
                    )
                previous = last_timestamp[source_name]
                if previous is not None and extracted_dt <= previous:
                    raise ValueError(
                        f"{source_name}: snapshot timestamps are not strictly increasing."
                    )
                last_timestamp[source_name] = extracted_dt

                prepared[source_name].append(
                    Snapshot(
                        run_id=run_id,
                        source_name=source_name,
                        path=snapshot_path,
                        extracted_at=extracted_at,
                        row_count=row_count,
                        sha256=digest,
                        manifest=manifest,
                    )
                )
                print(
                    f"PREFLIGHT {run_id} {source_name}: {row_count:,} rows; "
                    f"{extracted_at}; sha256 verified",
                    flush=True,
                )

    for source_name, snapshots in prepared.items():
        if len(snapshots) != len(RUN_IDS):
            raise ValueError(
                f"{source_name}: expected {len(RUN_IDS)} snapshots, found {len(snapshots)}."
            )
    return prepared


def _connection_parameters(uri: str) -> tuple[dict, bytes]:
    parts = urlsplit(uri)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    if parts.hostname.lower() != EXPECTED_ACCOUNT:
        raise ValueError(
            f"Unexpected Snowflake account {parts.hostname!r}; expected {EXPECTED_ACCOUNT!r}."
        )
    if parts.path.strip("/").upper() != EXPECTED_DATABASE:
        raise ValueError("Snowflake connection URI must target TORONTO.")
    encoded_key = params.get("private_key", [""])[0].replace(" ", "+")
    if not encoded_key:
        raise ValueError("Snowflake connection URI has no private_key parameter.")
    try:
        key_bytes = base64.b64decode(encoded_key, validate=True)
    except ValueError as exc:
        raise ValueError("Snowflake private_key is not valid Base64.") from exc
    if not key_bytes:
        raise ValueError("Snowflake private_key decoded to an empty value.")

    values = {
        "account": parts.hostname,
        "user": unquote(parts.username),
        "warehouse": params.get("warehouse", [""])[0],
        "role": params.get("role", [""])[0],
        "database": parts.path.strip("/"),
        "session_parameters": {"TIMEZONE": "UTC"},
        "login_timeout": 30,
    }
    if values["warehouse"] != EXPECTED_WAREHOUSE:
        raise ValueError("Snowflake connection URI has an unexpected warehouse.")
    if values["role"] != EXPECTED_ROLE:
        raise ValueError("Snowflake connection URI has an unexpected role.")
    return values, key_bytes


class SnowflakeSession:
    """Temporary key-file wrapper for the already-configured GitHub secret."""

    def __init__(self, uri: str):
        self.uri = uri
        self.temp_dir: tempfile.TemporaryDirectory | None = None
        self.connection = None

    def __enter__(self):
        values, key_bytes = _connection_parameters(self.uri)
        self.temp_dir = tempfile.TemporaryDirectory(prefix="toronto-snowflake-replay-")
        key_path = Path(self.temp_dir.name) / "snowflake_key.p8"
        key_path.write_bytes(key_bytes)
        key_path.chmod(0o600)
        values["private_key_file"] = str(key_path)
        passphrase = parse_qs(urlsplit(self.uri).query).get(
            "private_key_passphrase", [None]
        )[0]
        if passphrase:
            values["private_key_file_pwd"] = unquote(passphrase)
        try:
            self.connection = snowflake.connector.connect(**values)
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    "SELECT CURRENT_USER(), CURRENT_ORGANIZATION_NAME(), "
                    "CURRENT_ACCOUNT_NAME(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
                )
                user, organization, account, role, warehouse, database = (
                    cursor.fetchone()
                )
            finally:
                cursor.close()
            if f"{organization}-{account}".lower() != EXPECTED_ACCOUNT:
                raise ValueError(
                    "Connected Snowflake organization/account does not match the expected account."
                )
            if (
                role != EXPECTED_ROLE
                or warehouse != EXPECTED_WAREHOUSE
                or database != EXPECTED_DATABASE
            ):
                raise ValueError(
                    "Snowflake session context mismatch: "
                    f"role={role!r}, warehouse={warehouse!r}, database={database!r}."
                )
            print(
                f"SNOWFLAKE CONNECTION PASS: user={user}, account={organization}-{account}, "
                f"role={role}, warehouse={warehouse}, database={database}",
                flush=True,
            )
            return self
        except Exception:
            if self.connection is not None:
                self.connection.close()
            if self.temp_dir is not None:
                self.temp_dir.cleanup()
            raise

    def __exit__(self, exc_type, exc, traceback):
        if self.connection is not None:
            self.connection.close()
        if self.temp_dir is not None:
            self.temp_dir.cleanup()

    def create_schema(self, schema: str) -> None:
        schema = _identifier(schema, "Snowflake schema")
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s",
                (schema,),
            )
            if int(cursor.fetchone()[0]) != 0:
                raise ValueError(
                    f"Refusing to reuse existing schema TORONTO.{schema}; choose a fresh replay run."
                )
            cursor.execute(f'CREATE SCHEMA TORONTO."{schema}"')
        finally:
            cursor.close()
        print(f"Created isolated schema TORONTO.{schema}.", flush=True)

    def drop_schema(self, schema: str) -> None:
        schema = _identifier(schema, "Snowflake schema")
        cursor = self.connection.cursor()
        try:
            cursor.execute(f'DROP SCHEMA TORONTO."{schema}" CASCADE')
        finally:
            cursor.close()


def _make_smoke_parquets(
    root: Path, source: SourceConfig
) -> tuple[Path, Path, str, str]:
    source_tag = "ACTIVE" if source.name == "active_permits" else "CLEARED"
    changed_key = f"SCD2_SMOKE_{source_tag}_CHANGE"
    stable_key = f"SCD2_SMOKE_{source_tag}_STABLE"
    base_rows = [
        {
            "PERMIT_NUM": changed_key,
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "SCD2_TIMESTAMP_SMOKE",
            "STATUS": "ORIGINAL",
            "FIXTURE_VALUE": "same",
        },
        {
            "PERMIT_NUM": stable_key,
            "REVISION_NUM": "00",
            "PERMIT_TYPE": "SCD2_TIMESTAMP_SMOKE",
            "STATUS": "UNCHANGED",
            "FIXTURE_VALUE": "same",
        },
    ]
    changed_rows = [dict(row) for row in base_rows]
    changed_rows[0]["STATUS"] = "UPDATED"
    first_path = root / f"{source.name}_first.parquet"
    second_path = root / f"{source.name}_second.parquet"
    pq.write_table(Table.from_pylist(base_rows), first_path)
    pq.write_table(Table.from_pylist(changed_rows), second_path)
    return first_path, second_path, changed_key, stable_key


def _run_timestamp_smoke(
    *,
    session: SnowflakeSession,
    sources: dict[str, SourceConfig],
    workflow_run_id: str,
    attempt: str,
    credentials: str,
) -> str:
    schema = f"SNOWFLAKE_SCD2_TIMESTAMP_TEST_{workflow_run_id}_{attempt}"
    schema = _identifier(schema, "timestamp-test schema")
    session.create_schema(schema)
    passed = False
    try:
        with tempfile.TemporaryDirectory(
            prefix="toronto-scd2-timestamp-smoke-"
        ) as temp:
            root = Path(temp)
            for source_name in SOURCE_NAMES:
                source = replace(
                    sources[source_name],
                    warehouse_since=None,
                    warehouse_date_column=None,
                )
                first_path, second_path, changed_key, stable_key = _make_smoke_parquets(
                    root, source
                )
                pipeline_name = (
                    f"toronto_ts_smoke_{source_name}_{workflow_run_id}_{attempt}"
                )
                load_scd2(
                    credentials=credentials,
                    snapshot_path=first_path,
                    source=source,
                    dataset_name=schema,
                    destination_kind="snowflake",
                    pipeline_name=pipeline_name,
                    boundary_timestamp=TIMESTAMP_ONE,
                )
                load_scd2(
                    credentials=credentials,
                    snapshot_path=second_path,
                    source=source,
                    dataset_name=schema,
                    destination_kind="snowflake",
                    pipeline_name=pipeline_name,
                    boundary_timestamp=TIMESTAMP_TWO,
                )

                cursor = session.connection.cursor()
                try:
                    table = f'TORONTO."{schema}"."{_identifier(source.warehouse_table, "table")}"'
                    cursor.execute(
                        f"SELECT PERMIT_NUM, STATUS, "
                        f"TO_VARCHAR(_DLT_VALID_FROM, '{TIMESTAMP_FORMAT}'), "
                        f"TO_VARCHAR(_DLT_VALID_TO, '{TIMESTAMP_FORMAT}') "
                        f"FROM {table} "
                        "ORDER BY PERMIT_NUM, _DLT_VALID_FROM"
                    )
                    rows = cursor.fetchall()
                finally:
                    cursor.close()

                expected = {
                    (
                        changed_key,
                        "ORIGINAL",
                        TIMESTAMP_ONE.replace("+00:00", ""),
                        TIMESTAMP_TWO.replace("+00:00", ""),
                    ),
                    (changed_key, "UPDATED", TIMESTAMP_TWO.replace("+00:00", ""), None),
                    (
                        stable_key,
                        "UNCHANGED",
                        TIMESTAMP_ONE.replace("+00:00", ""),
                        None,
                    ),
                }
                actual = {(str(row[0]), str(row[1]), row[2], row[3]) for row in rows}
                if len(rows) != 3 or actual != expected:
                    raise ValueError(
                        f"{source_name}: Snowflake boundary timestamp smoke failed; "
                        f"expected={sorted(expected)!r}, actual={sorted(actual, key=str)!r}"
                    )
                print(
                    f"PASS {source_name}: Snowflake stored both exact historical "
                    "valid-from/valid-to boundaries.",
                    flush=True,
                )
        passed = True
    finally:
        if passed:
            session.drop_schema(schema)
            print(
                f"Cleaned up successful timestamp-test schema TORONTO.{schema}.",
                flush=True,
            )
        else:
            print(
                f"Timestamp smoke failed; preserving TORONTO.{schema} for diagnosis.",
                flush=True,
            )
    return schema


def _source_signature_rows(
    snapshot: Snapshot, source: SourceConfig
) -> tuple[dict, set]:
    key_columns = _resolve_scd2_key_columns(snapshot.path, source)
    conflicts, _ = _validate_scd2_snapshot(
        snapshot.path,
        source=source,
        key_columns=key_columns,
        preserve_row_hash=False,
    )
    quarantined = {tuple(conflict[0]) for conflict in conflicts}
    rows_by_key = {}
    for row in _iter_scd2_snapshot_rows(
        snapshot.path,
        key_columns=key_columns,
        date_column=source.warehouse_date_column,
        since=source.warehouse_since,
        preserve_row_hash=False,
        source_name=source.name,
        quarantined_keys=quarantined,
    ):
        key = tuple(row[column] for column in key_columns)
        rows_by_key[tuple(str(value) for value in key)] = str(row["row_hash"])
    return rows_by_key, {tuple(str(value) for value in key) for key in quarantined}


def _snowflake_current_map(
    session: SnowflakeSession,
    schema: str,
    source: SourceConfig,
) -> dict[tuple[str, ...], str]:
    keys = [_identifier(column, "business-key column") for column in source.primary_key]
    table = f'TORONTO."{schema}"."{_identifier(source.warehouse_table, "table")}"'
    select_keys = ", ".join(f"TO_VARCHAR({column})" for column in keys)
    cursor = session.connection.cursor()
    try:
        cursor.execute(
            f"SELECT {select_keys}, ROW_HASH FROM {table} WHERE _DLT_VALID_TO IS NULL"
        )
        result: dict[tuple[str, ...], str] = {}
        for row in cursor:
            key = tuple(str(value) for value in row[:-1])
            if key in result:
                raise ValueError(
                    f"{source.name}: duplicate current business key in Snowflake: {key!r}"
                )
            result[key] = str(row[-1])
        return result
    finally:
        cursor.close()


def _table_metrics(
    session: SnowflakeSession,
    schema: str,
    source: SourceConfig,
) -> dict:
    table = f'TORONTO."{schema}"."{_identifier(source.warehouse_table, "table")}"'
    cursor = session.connection.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*), "
            "COALESCE(COUNT_IF(_DLT_VALID_TO IS NULL), 0), "
            "COALESCE(COUNT_IF(_DLT_VALID_TO IS NOT NULL), 0), "
            "COALESCE(COUNT_IF(_DLT_VALID_FROM IS NULL), 0), "
            "COALESCE(COUNT_IF(_DLT_VALID_TO IS NOT NULL AND _DLT_VALID_TO <= _DLT_VALID_FROM), 0) "
            f"FROM {table}"
        )
        total, current, expired, null_from, invalid_intervals = cursor.fetchone()

        keys = [
            _identifier(column, "business-key column") for column in source.primary_key
        ]
        key_sql = ", ".join(keys)
        cursor.execute(
            f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM {table} "
            "WHERE _DLT_VALID_TO IS NULL "
            f"GROUP BY {key_sql} HAVING COUNT(*) > 1)"
        )
        duplicate_current = int(cursor.fetchone()[0])

        cursor.execute(
            "WITH ordered_versions AS ("
            f"SELECT {key_sql}, _DLT_VALID_FROM, "
            "LAG(_DLT_VALID_TO) OVER ("
            f"PARTITION BY {key_sql} ORDER BY _DLT_VALID_FROM) AS PREVIOUS_VALID_TO "
            f"FROM {table}) "
            "SELECT COUNT(*) FROM ordered_versions "
            "WHERE PREVIOUS_VALID_TO IS NOT NULL "
            "AND PREVIOUS_VALID_TO > _DLT_VALID_FROM"
        )
        overlaps = int(cursor.fetchone()[0])
    finally:
        cursor.close()

    return {
        "total": int(total),
        "current": int(current),
        "expired": int(expired),
        "null_valid_from": int(null_from),
        "invalid_intervals": int(invalid_intervals),
        "duplicate_current_business_keys": duplicate_current,
        "overlapping_intervals": overlaps,
    }


def _export_history(
    session: SnowflakeSession,
    schema: str,
    source: SourceConfig,
    output_path: Path,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    keys = [_identifier(column, "business-key column") for column in source.primary_key]
    table = f'TORONTO."{schema}"."{_identifier(source.warehouse_table, "table")}"'
    fields = [f"TO_VARCHAR({column})" for column in keys]
    fields.extend(
        [
            "TO_VARCHAR(ROW_HASH)",
            f"TO_VARCHAR(_DLT_VALID_FROM, '{TIMESTAMP_FORMAT}')",
            f"TO_VARCHAR(_DLT_VALID_TO, '{TIMESTAMP_FORMAT}')",
        ]
    )
    order_by = ", ".join([*keys, "_DLT_VALID_FROM", "ROW_HASH"])
    query = f"SELECT {', '.join(fields)} FROM {table} ORDER BY {order_by}"
    cursor = session.connection.cursor()
    count = 0
    try:
        cursor.execute(query)
        with gzip.open(output_path, "wt", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow([*keys, "row_hash", "valid_from", "valid_to"])
            while True:
                rows = cursor.fetchmany(10_000)
                if not rows:
                    break
                writer.writerows(rows)
                count += len(rows)
    finally:
        cursor.close()
    return count


def main() -> None:
    app = load_config(Path("open_data_config.yml"))
    if app.warehouse_kind != "snowflake":
        raise ValueError("TORONTO_WAREHOUSE_KIND must be snowflake.")
    credentials = app.warehouse_credentials
    if not credentials:
        raise ValueError("TORONTO_WAREHOUSE_CREDENTIALS is missing.")

    schema = _identifier(app.dataset_name, "target dataset")
    if not re.fullmatch(r"SNOWFLAKE_SCD2_REBUILD_20261107_[0-9]+_[0-9]+", schema):
        raise ValueError(
            "Refusing to replay outside the isolated historical-rebuild schema."
        )

    workflow_run_id = os.environ.get("GITHUB_RUN_ID", "")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
    for label, value in (
        ("GITHUB_RUN_ID", workflow_run_id),
        ("GITHUB_RUN_ATTEMPT", attempt),
    ):
        if not re.fullmatch(r"[0-9]+", value):
            raise ValueError(f"{label} must contain digits only.")

    sources = {source.name: source for source in app.sources}
    for source_name in SOURCE_NAMES:
        source = sources.get(source_name)
        if (
            source is None
            or not source.warehouse_load
            or source.warehouse_strategy != "scd2"
        ):
            raise ValueError(f"{source_name}: expected an enabled SCD2 source.")

    bundle_root = Path("data/recovery/scd2-rebuild/snapshot-bundles")
    prepared_root = Path("data/recovery/scd2-rebuild/prepared-snapshots")
    bundle_root.mkdir(parents=True, exist_ok=True)
    prepared_root.mkdir(parents=True, exist_ok=True)
    metadata = _download_bundles(bundle_root)
    prepared = _extract_and_validate_bundles(
        bundle_root, prepared_root, metadata, sources
    )

    evidence_dir = Path("data/recovery/scd2-rebuild/snowflake-replay-evidence")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    summary: dict = {
        "status": "running",
        "repository": REPOSITORY,
        "workflow_run_id": workflow_run_id,
        "workflow_attempt": attempt,
        "snowflake_account": EXPECTED_ACCOUNT,
        "database": EXPECTED_DATABASE,
        "schema": schema,
        "snapshot_run_ids": list(RUN_IDS),
        "snapshot_count": len(RUN_IDS),
        "created_at_utc": datetime.now(UTC).isoformat(),
        "sources": {},
    }

    with SnowflakeSession(credentials) as session:
        smoke_schema = _run_timestamp_smoke(
            session=session,
            sources=sources,
            workflow_run_id=workflow_run_id,
            attempt=attempt,
            credentials=credentials,
        )
        summary["timestamp_smoke_schema_cleaned"] = smoke_schema

        session.create_schema(schema)
        for source_name in SOURCE_NAMES:
            source = sources[source_name]
            snapshots = prepared[source_name]
            load_log = []
            for index, snapshot in enumerate(snapshots, start=1):
                result = load_scd2(
                    credentials=credentials,
                    snapshot_path=snapshot.path,
                    source=source,
                    dataset_name=schema,
                    destination_kind="snowflake",
                    pipeline_name=f"toronto_replay_{source_name}_20261107_{workflow_run_id}_{attempt}",
                    boundary_timestamp=snapshot.extracted_at,
                )
                load_log.append(
                    {
                        "run_id": snapshot.run_id,
                        "extracted_at_utc": snapshot.extracted_at,
                        "source_rows": snapshot.row_count,
                        "sha256": snapshot.sha256,
                        "quarantined_business_keys": len(
                            result.quarantined_business_keys
                        ),
                        "quarantined_rows": result.quarantined_row_count,
                    }
                )
                print(
                    f"LOADED {source_name} snapshot {index}/{len(snapshots)} "
                    f"run={snapshot.run_id} rows={snapshot.row_count:,} "
                    f"quarantined_keys={len(result.quarantined_business_keys)} "
                    f"quarantined_rows={result.quarantined_row_count}",
                    flush=True,
                )

            metrics = _table_metrics(session, schema, source)
            expected = EXPECTED_METRICS[source_name]
            actual_counts = {
                key: metrics[key] for key in ("total", "current", "expired")
            }
            if actual_counts != expected:
                raise ValueError(
                    f"{source_name}: final row counts differ from validated DuckDB replay; "
                    f"expected={expected}, actual={actual_counts}"
                )
            if metrics["total"] != metrics["current"] + metrics["expired"]:
                raise ValueError(
                    f"{source_name}: SCD2 total/current/expired counts do not reconcile."
                )
            for check in (
                "null_valid_from",
                "invalid_intervals",
                "duplicate_current_business_keys",
                "overlapping_intervals",
            ):
                if metrics[check] != 0:
                    raise ValueError(
                        f"{source_name}: {check}={metrics[check]}, expected zero."
                    )

            latest = snapshots[-1]
            expected_current, latest_quarantined = _source_signature_rows(
                latest, source
            )
            actual_current = _snowflake_current_map(session, schema, source)
            missing_or_changed = {
                key: (row_hash, actual_current.get(key))
                for key, row_hash in expected_current.items()
                if actual_current.get(key) != row_hash
            }
            unexpected = set(actual_current) - set(expected_current)
            unexpected_non_quarantined = unexpected - latest_quarantined
            if missing_or_changed or unexpected_non_quarantined:
                raise ValueError(
                    f"{source_name}: latest source/current Snowflake key-hash parity failed; "
                    f"missing_or_changed={len(missing_or_changed)}, "
                    f"unexpected_non_quarantined={len(unexpected_non_quarantined)}"
                )
            if len(actual_current) != expected["current"]:
                raise ValueError(
                    f"{source_name}: current row count changed during source parity check."
                )

            export_path = evidence_dir / f"{source.warehouse_table}_history.csv.gz"
            export_count = _export_history(session, schema, source, export_path)
            if export_count != metrics["total"]:
                raise ValueError(f"{source_name}: exported history row count mismatch.")

            summary["sources"][source_name] = {
                "table": source.warehouse_table,
                "snapshots_loaded": len(snapshots),
                "metrics": metrics,
                "expected_metrics": expected,
                "latest_source_unambiguous_rows": len(expected_current),
                "latest_source_quarantined_keys": len(latest_quarantined),
                "latest_current_key_hash_parity": "passed",
                "snapshot_loads": load_log,
                "history_export": str(export_path),
                "history_export_rows": export_count,
            }
            print(
                f"PASS {source_name}: {metrics['total']:,} history rows, "
                f"{metrics['current']:,} current, {metrics['expired']:,} expired; "
                "no duplicate-current keys or overlapping intervals; latest source hashes match.",
                flush=True,
            )

    summary["status"] = "passed"
    summary["completed_at_utc"] = datetime.now(UTC).isoformat()
    summary_path = evidence_dir / "replay-summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"HISTORICAL SCD2 REPLAY PASSED: TORONTO.{schema}", flush=True)
    print(f"Evidence: {evidence_dir}", flush=True)


if __name__ == "__main__":
    main()
