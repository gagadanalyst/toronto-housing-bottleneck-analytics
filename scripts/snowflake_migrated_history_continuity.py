"""Fast, isolated verification that dlt continues from migrated Snowflake SCD2 history.

Uses a 500-row sample per source from the exact DuckDB recovery artifact used for
migration. One non-key STATUS value is deliberately changed to prove that a
migrated current version is expired and replaced. The identical sample is then
replayed once to prove idempotence. Live CKAN extraction completeness is covered
by the separate, manually triggered Snowflake Live Ingestion Smoke workflow.
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, unquote, urlsplit

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_der_private_key,
    load_pem_private_key,
)

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2

DATABASE = "TORONTO"
SOURCE_SCHEMA = "RAW"
SCD2_TABLES = ("RAW_ACTIVE_PERMITS", "RAW_CLEARED_PERMITS")
REQUIRED_TABLES = (
    "RAW_ACTIVE_PERMITS",
    "RAW_CLEARED_PERMITS",
    "_DLT_LOADS",
    "_DLT_VERSION",
    "_DLT_PIPELINE_STATE",
)
SOURCE_NAMES = ("active_permits", "cleared_permits")
PIPELINE_NAMES = {"TORONTO_ACTIVE_PERMITS_SCD2", "TORONTO_CLEARED_PERMITS_SCD2"}
SAMPLE_ROWS = 500


def _safe_identifier(value: str, label: str = "identifier") -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _safe_schema(value: str) -> str:
    normalized = value.upper()
    if not re.fullmatch(r"SNOWFLAKE_CONTINUITY_[A-Z0-9_]+", normalized):
        raise ValueError("Refusing to run outside SNOWFLAKE_CONTINUITY_<run-id>.")
    return normalized


def _decode_private_key(encoded_key: str, passphrase: str | None) -> bytes:
    """Decode dlt-supported PEM/DER key representations to connector-ready DER."""
    password = passphrase.encode("utf-8") if passphrase else None
    candidates = [encoded_key]
    if "-----BEGIN" not in encoded_key:
        candidates.extend((encoded_key.replace(" ", "+"), "".join(encoded_key.split())))

    attempted: set[bytes] = set()
    for candidate in candidates:
        raw = candidate.encode("utf-8")
        if raw not in attempted:
            attempted.add(raw)
            for loader in (load_pem_private_key, load_der_private_key):
                try:
                    key = loader(raw, password=password)
                    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
                except (TypeError, ValueError):
                    pass
        try:
            decoded = base64.b64decode(candidate, validate=False)
        except (ValueError, base64.binascii.Error):
            continue
        if decoded in attempted:
            continue
        attempted.add(decoded)
        for loader in (load_pem_private_key, load_der_private_key):
            try:
                key = loader(decoded, password=password)
                return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
            except (TypeError, ValueError):
                pass

    raise ValueError(
        "Unable to decode Snowflake private_key. Check the key format, URL encoding, "
        "and private_key_passphrase."
    )


def _connect(uri: str):
    parts = urlsplit(uri)
    query = parse_qs(parts.query, keep_blank_values=True)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("Expected a Snowflake dlt connection URI.")
    encoded_key = query.get("private_key", [None])[0]
    if not encoded_key:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION has no private_key parameter.")
    database = unquote(parts.path.strip("/").split("/")[0])
    if database.upper() != DATABASE:
        raise ValueError(f"Expected database {DATABASE}; found {database!r}.")
    if not query.get("warehouse") or not query.get("role"):
        raise ValueError("Snowflake URI must specify warehouse and role.")
    key_der = _decode_private_key(encoded_key, query.get("private_key_passphrase", [None])[0])
    return snowflake.connector.connect(
        account=parts.hostname,
        user=unquote(parts.username),
        private_key=key_der,
        database=DATABASE,
        schema=SOURCE_SCHEMA,
        warehouse=query["warehouse"][0],
        role=query["role"][0],
        application="TORONTO_MIGRATED_HISTORY_CONTINUITY_TEST",
    )


def _table_inventory(conn, schema: str) -> set[str]:
    cursor = conn.cursor()
    try:
        return {
            row[0]
            for row in cursor.execute(
                "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{schema}'"
            ).fetchall()
        }
    finally:
        cursor.close()


def _columns(conn, schema: str, table: str) -> set[str]:
    if table not in REQUIRED_TABLES:
        raise ValueError(f"Unexpected table: {table}")
    cursor = conn.cursor()
    try:
        return {
            row[0]
            for row in cursor.execute(
                "SELECT COLUMN_NAME FROM TORONTO.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
            ).fetchall()
        }
    finally:
        cursor.close()


def _state_pipeline_names(conn, schema: str) -> set[str]:
    columns = _columns(conn, schema, "_DLT_PIPELINE_STATE")
    if "PIPELINE_NAME" not in columns:
        raise RuntimeError(f"{schema}._DLT_PIPELINE_STATE has no PIPELINE_NAME column.")
    cursor = conn.cursor()
    try:
        return {
            str(row[0]).upper()
            for row in cursor.execute(
                f"SELECT DISTINCT PIPELINE_NAME FROM {DATABASE}.{schema}._DLT_PIPELINE_STATE "
                "WHERE PIPELINE_NAME IS NOT NULL"
            ).fetchall()
        }
    finally:
        cursor.close()


def _counts(conn, schema: str, table: str) -> tuple[int, int, int]:
    if table not in SCD2_TABLES:
        raise ValueError(f"Unexpected SCD2 table: {table}")
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
            f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) FROM {DATABASE}.{schema}.{table}"
        ).fetchone()
    finally:
        cursor.close()
    result = tuple(int(value or 0) for value in row)
    if result[0] != result[1] + result[2]:
        raise AssertionError(f"{schema}.{table}: total/current/expired counts do not reconcile: {result}.")
    return result  # type: ignore[return-value]


def _row_count(conn, schema: str, table: str) -> int:
    if table not in REQUIRED_TABLES:
        raise ValueError(f"Unexpected metadata table: {table}")
    cursor = conn.cursor()
    try:
        return int(cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{schema}.{table}").fetchone()[0])
    finally:
        cursor.close()


def _duckdb_counts(db: duckdb.DuckDBPyConnection, table: str) -> tuple[int, int, int]:
    table = _safe_identifier(table, "DuckDB table")
    row = db.execute(
        f'SELECT COUNT(*), COUNT(*) FILTER (WHERE "_dlt_valid_to" IS NULL), '
        f'COUNT(*) FILTER (WHERE "_dlt_valid_to" IS NOT NULL) FROM raw.{_quoted_identifier(table)}'
    ).fetchone()
    return tuple(int(value or 0) for value in row)  # type: ignore[return-value]


def _duckdb_row_count(db: duckdb.DuckDBPyConnection, table: str) -> int:
    table = _safe_identifier(table, "DuckDB metadata table")
    return int(db.execute(f'SELECT COUNT(*) FROM raw.{_quoted_identifier(table)}').fetchone()[0])


def _export_sample_and_change_one(
    *, source_db: Path, source, output_path: Path, sample_rows: int = SAMPLE_ROWS
) -> int:
    """Export a bounded current-row sample and change one non-key STATUS value."""
    table = _safe_identifier(source.warehouse_table, "source warehouse table")
    with duckdb.connect(str(source_db), read_only=True) as db:
        described = db.execute(f'DESCRIBE SELECT * FROM raw.{_quoted_identifier(table)}').fetchall()
        columns = [str(row[0]) for row in described]
        by_lower = {name.lower(): name for name in columns}
        valid_to = by_lower.get("_dlt_valid_to")
        status_col = by_lower.get("status")
        if not valid_to or not status_col:
            raise RuntimeError(f"raw.{table} must contain _dlt_valid_to and STATUS.")
        business_columns = [
            name for name in columns
            if not name.lower().startswith("_dlt_") and name.lower() != "row_hash"
        ]
        missing_keys = [key for key in source.primary_key if key.lower() not in by_lower]
        if missing_keys:
            raise RuntimeError(f"raw.{table} is missing configured primary-key columns: {missing_keys}")
        projection = ", ".join(_quoted_identifier(name) for name in business_columns)
        order_by = ", ".join(_quoted_identifier(by_lower[key.lower()]) for key in source.primary_key)
        arrow_table = db.execute(
            f"SELECT {projection} FROM raw.{_quoted_identifier(table)} "
            f"WHERE {_quoted_identifier(valid_to)} IS NULL ORDER BY {order_by} LIMIT {int(sample_rows)}"
        ).to_arrow_table()

    if arrow_table.num_rows < 1:
        raise RuntimeError(f"raw.{table} has no current rows to sample.")
    status_index = arrow_table.schema.get_field_index(status_col)
    status_values = arrow_table.column(status_index).to_pylist()
    original_status = status_values[0]
    status_values[0] = f"{original_status or 'UNKNOWN'}__CONTINUITY_TEST__"
    field = arrow_table.schema.field(status_index)
    arrow_table = arrow_table.set_column(status_index, field, pa.array(status_values, type=field.type))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_table, output_path, compression="zstd")
    print(
        f"SAMPLED {table}: {arrow_table.num_rows:,} current rows; changed STATUS for "
        f"one row while preserving its primary key; included {arrow_table.num_columns} business columns."
    )
    return int(arrow_table.num_rows)


def _load_sample(credentials: str, dataset: str, source, path: Path) -> None:
    result = load_scd2(
        credentials=credentials,
        snapshot_path=path,
        source=source,
        dataset_name=dataset,
        destination_kind="snowflake",
    )
    result.load_info.raise_on_failed_jobs()


def main() -> None:
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    if os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower() != "snowflake":
        raise SystemExit("Set TORONTO_WAREHOUSE_KIND=snowflake for this test.")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")
    dataset = _safe_schema(os.environ.get("TORONTO_WAREHOUSE_DATASET", ""))
    source_db = Path(os.environ.get("SOURCE_DB", "data/recovery/warehouse-2026-10-23/toronto.duckdb"))
    if not source_db.is_file():
        raise SystemExit(f"Recovery DuckDB database not found: {source_db}")

    app = load_config(Path("open_data_config.yml"))
    sources = {source.name: source for source in app.sources}
    source_map = {name: sources[name] for name in SOURCE_NAMES}

    conn = _connect(credentials)
    schema_created = False
    try:
        cursor = conn.cursor()
        try:
            database, current_schema, role = cursor.execute(
                "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()"
            ).fetchone()
            print(f"Connected as {role} to {database}.{current_schema}.")
            if database.upper() != DATABASE or role.upper() != "TORONTO_INGEST_ROLE":
                raise RuntimeError("Unexpected Snowflake database or role; stopping before cloning.")
        finally:
            cursor.close()

        available = _table_inventory(conn, SOURCE_SCHEMA)
        missing = set(REQUIRED_TABLES) - available
        if missing:
            raise RuntimeError(f"TORONTO.RAW is missing migrated tables: {sorted(missing)}.")
        prior = {source_map[name].warehouse_table.upper(): _counts(conn, SOURCE_SCHEMA, source_map[name].warehouse_table.upper()) for name in SOURCE_NAMES}
        metadata_prior = {table: _row_count(conn, SOURCE_SCHEMA, table) for table in REQUIRED_TABLES if table.startswith("_DLT_")}
        state_names = _state_pipeline_names(conn, SOURCE_SCHEMA)
        if not PIPELINE_NAMES.issubset(state_names):
            raise RuntimeError(f"Missing migrated pipeline state: {sorted(PIPELINE_NAMES - state_names)}")

        # Verify the supplied recovery DB is exactly the baseline that was migrated.
        with duckdb.connect(str(source_db), read_only=True) as db:
            for name in SOURCE_NAMES:
                table = source_map[name].warehouse_table
                expected = _duckdb_counts(db, table)
                if expected != prior[table.upper()]:
                    raise AssertionError(
                        f"Recovery baseline mismatch for {table}: DuckDB={expected}; Snowflake={prior[table.upper()]}"
                    )
            for table, count in metadata_prior.items():
                if _duckdb_row_count(db, table.lower()) != count:
                    raise AssertionError(f"Recovery metadata count mismatch for {table}.")

        print(f"MIGRATED BASELINE: {prior}")
        print(f"MIGRATED METADATA ROWS: {metadata_prior}")

        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{dataset} CASCADE")
            cursor.execute(f"CREATE SCHEMA {DATABASE}.{dataset} CLONE {DATABASE}.{SOURCE_SCHEMA}")
            schema_created = True
        finally:
            cursor.close()
        print(f"CLONED {DATABASE}.{SOURCE_SCHEMA} to isolated {DATABASE}.{dataset}.")

        cloned_states = _state_pipeline_names(conn, dataset)
        if not PIPELINE_NAMES.issubset(cloned_states):
            raise RuntimeError(f"Clone is missing pipeline state: {sorted(PIPELINE_NAMES - cloned_states)}")
        for table, expected in prior.items():
            actual = _counts(conn, dataset, table)
            if actual != expected:
                raise AssertionError(f"Clone mismatch for {table}: expected={expected}; actual={actual}")
        cloned_metadata = {table: _row_count(conn, dataset, table) for table in metadata_prior}
        if cloned_metadata != metadata_prior:
            raise AssertionError(f"Cloned metadata mismatch: expected={metadata_prior}; actual={cloned_metadata}")
        print(f"CLONE VERIFIED: SCD2 counts={prior}; metadata={cloned_metadata}")

        with TemporaryDirectory(prefix="snowflake-continuity-sample-") as temp_dir:
            temp = Path(temp_dir)
            snapshots: dict[str, Path] = {}
            for name in SOURCE_NAMES:
                source = source_map[name]
                snapshot = temp / f"{name}.parquet"
                _export_sample_and_change_one(source_db=source_db, source=source, output_path=snapshot)
                snapshots[name] = snapshot

            # Release the direct SQL session while dlt opens its own destination session(s).
            conn.close()
            conn = None
            for name in SOURCE_NAMES:
                _load_sample(credentials, dataset, source_map[name], snapshots[name])

            conn = _connect(credentials)
            after_load: dict[str, tuple[int, int, int]] = {}
            for name in SOURCE_NAMES:
                table = source_map[name].warehouse_table.upper()
                before = prior[table]
                expected_after = (before[0] + 1, before[1], before[2] + 1)
                actual = _counts(conn, dataset, table)
                if actual != expected_after:
                    raise AssertionError(
                        f"{table}: expected exactly one migrated version change. "
                        f"Before={before}; expected after={expected_after}; actual={actual}. "
                        "Extra versions may indicate row-hash or type incompatibility."
                    )
                after_load[table] = actual
                print(f"PASS migrated-history change detection {name}: before={before}; after={actual}")

            metadata_after = {table: _row_count(conn, dataset, table) for table in metadata_prior}
            for table, count in metadata_prior.items():
                if metadata_after[table] < count:
                    raise AssertionError(f"{table}: metadata count decreased from {count} to {metadata_after[table]}.")
            state_names_after = _state_pipeline_names(conn, dataset)
            if not PIPELINE_NAMES.issubset(state_names_after):
                raise AssertionError(f"Pipeline state records missing after load: {sorted(PIPELINE_NAMES - state_names_after)}")
            print(f"METADATA AFTER LOAD: {metadata_after}")

            conn.close()
            conn = None
            # One identical replay is sufficient here; the standalone live smoke checks two replays.
            for name in SOURCE_NAMES:
                _load_sample(credentials, dataset, source_map[name], snapshots[name])
            conn = _connect(credentials)
            for name in SOURCE_NAMES:
                table = source_map[name].warehouse_table.upper()
                actual = _counts(conn, dataset, table)
                if actual != after_load[table]:
                    raise AssertionError(f"{table}: identical sample replay changed counts: before replay={after_load[table]}; after={actual}")
                print(f"PASS identical replay {name}: total={actual[0]:,}, current={actual[1]:,}, expired={actual[2]:,}")

        print(
            "SNOWFLAKE MIGRATED-HISTORY CONTINUITY PASSED: 500-row samples from the exact "
            "migration baseline preserved row hashes, one changed key created one new version, "
            "and one identical replay added no further versions."
        )
    finally:
        if schema_created:
            try:
                if conn is None:
                    conn = _connect(credentials)
                cursor = conn.cursor()
                try:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{dataset} CASCADE")
                finally:
                    cursor.close()
                print(f"Cleaned up isolated schema {DATABASE}.{dataset}.", flush=True)
            except Exception as exc:
                print(f"WARNING: unable to clean up isolated schema {DATABASE}.{dataset}: {type(exc).__name__}: {exc}", flush=True)
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
