"""Fast, isolated verification that dlt continues from migrated Snowflake SCD2 history.

Uses 500 business keys per source from the exact DuckDB recovery artifact used for
migration. The isolated Snowflake clone is reduced to the complete migrated history
for those keys before loading, so dlt never treats unrelated current rows as absent.
One non-key STATUS value is deliberately changed, then the identical snapshot is
replayed once to prove idempotence. Live CKAN extraction completeness is covered by
the separate, manually triggered Snowflake Live Ingestion Smoke workflow.
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
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
    if table not in set(REQUIRED_TABLES) | set(SCD2_TABLES):
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


def _normalized_key(values: tuple[Any, ...]) -> tuple[str | None, ...]:
    """Normalize DB-driver scalar types so DuckDB and Snowflake keys compare equally."""
    return tuple(None if value is None else str(value) for value in values)


def _key_rows(keys: pa.Table) -> list[tuple[Any, ...]]:
    names = list(keys.schema.names)
    rows = [tuple(row[name] for name in names) for row in keys.to_pylist()]
    if not rows:
        raise RuntimeError("The continuity sample has no business keys.")
    if any(value is None for row in rows for value in row):
        raise RuntimeError("The continuity sample contains a NULL primary-key value.")
    if len({_normalized_key(row) for row in rows}) != len(rows):
        raise RuntimeError("The continuity sample contains duplicate business keys.")
    return rows


def _values_relation(keys: pa.Table, target_columns: list[str]) -> tuple[str, tuple[Any, ...]]:
    """Build a parameterized Snowflake VALUES relation for selected business keys."""
    rows = _key_rows(keys)
    if len(target_columns) != len(keys.schema.names):
        raise ValueError("Selected-key column count does not match the configured primary key.")
    row_placeholder = "(" + ", ".join(["%s"] * len(target_columns)) + ")"
    values_sql = ", ".join([row_placeholder] * len(rows))
    columns_sql = ", ".join(_quoted_identifier(column) for column in target_columns)
    relation = f"(VALUES {values_sql}) AS selected_keys ({columns_sql})"
    params = tuple(value for row in rows for value in row)
    return relation, params


def _selected_snowflake_key_columns(conn, schema: str, source) -> list[str]:
    table = source.warehouse_table.upper()
    by_lower = {column.lower(): column for column in _columns(conn, schema, table)}
    missing = [key for key in source.primary_key if key.lower() not in by_lower]
    if missing:
        raise RuntimeError(f"{schema}.{table} is missing configured key columns: {missing}")
    return [by_lower[key.lower()] for key in source.primary_key]


def _restrict_clone_to_selected_keys(conn, schema: str, source, keys: pa.Table) -> None:
    """Keep complete SCD2 histories for sample keys only, inside the isolated clone."""
    table = source.warehouse_table.upper()
    key_columns = _selected_snowflake_key_columns(conn, schema, source)
    relation, params = _values_relation(keys, key_columns)
    join = " AND ".join(
        f"target.{_quoted_identifier(column)} = selected_keys.{_quoted_identifier(column)}"
        for column in key_columns
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"DELETE FROM {DATABASE}.{schema}.{table} target "
            f"WHERE NOT EXISTS (SELECT 1 FROM {relation} WHERE {join})",
            params,
        )
    finally:
        cursor.close()
    print(
        f"ISOLATED {table}: retained {keys.num_rows:,} selected business keys and their "
        "complete SCD2 histories; unrelated keys were removed from the test clone."
    )


def _snowflake_history_by_key(conn, schema: str, source, keys: pa.Table) -> dict[tuple[str | None, ...], tuple[int, int, int]]:
    """Return total/current/expired counts for each selected key in the isolated table."""
    table = source.warehouse_table.upper()
    key_columns = _selected_snowflake_key_columns(conn, schema, source)
    relation, params = _values_relation(keys, key_columns)
    projection = ", ".join(f"target.{_quoted_identifier(column)}" for column in key_columns)
    join = " AND ".join(
        f"target.{_quoted_identifier(column)} = selected_keys.{_quoted_identifier(column)}"
        for column in key_columns
    )
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            f"SELECT {projection}, COUNT(*), COUNT_IF(target._DLT_VALID_TO IS NULL), "
            f"COUNT_IF(target._DLT_VALID_TO IS NOT NULL) "
            f"FROM {DATABASE}.{schema}.{table} AS target "
            f"JOIN {relation} ON {join} GROUP BY {projection}",
            params,
        ).fetchall()
    finally:
        cursor.close()
    history = {
        _normalized_key(tuple(row[: len(key_columns)])): tuple(
            int(value or 0) for value in row[len(key_columns) : len(key_columns) + 3]
        )
        for row in rows
    }
    if len(history) != keys.num_rows:
        raise AssertionError(
            f"{schema}.{table}: expected histories for {keys.num_rows} selected keys, "
            f"found {len(history)}."
        )
    for key, counts in history.items():
        if counts[0] != counts[1] + counts[2] or counts[1] != 1:
            raise AssertionError(
                f"{schema}.{table}: invalid baseline history for key {key}: {counts}; "
                "each selected key must have one current version and reconciled history."
            )
    return history


def _duckdb_history_by_key(source_db: Path, source, keys: pa.Table) -> dict[tuple[str | None, ...], tuple[int, int, int]]:
    """Return full recovered-history counts for each selected business key."""
    table = _safe_identifier(source.warehouse_table, "DuckDB table")
    with duckdb.connect(str(source_db), read_only=True) as db:
        described = db.execute(f'DESCRIBE SELECT * FROM raw.{_quoted_identifier(table)}').fetchall()
        by_lower = {str(row[0]).lower(): str(row[0]) for row in described}
        key_columns = [by_lower[key.lower()] for key in source.primary_key]
        key_arrow_columns = [next(name for name in keys.schema.names if name.lower() == col.lower()) for col in key_columns]
        registered_keys = keys.select(key_arrow_columns).rename_columns(key_columns)
        db.register("continuity_selected_keys", registered_keys)
        projection = ", ".join(f"target.{_quoted_identifier(column)}" for column in key_columns)
        join = " AND ".join(
            f"target.{_quoted_identifier(column)} = selected_keys.{_quoted_identifier(column)}"
            for column in key_columns
        )
        rows = db.execute(
            f"SELECT {projection}, COUNT(*), "
            f'COUNT(*) FILTER (WHERE target."_dlt_valid_to" IS NULL), '
            f'COUNT(*) FILTER (WHERE target."_dlt_valid_to" IS NOT NULL) '
            f'FROM raw.{_quoted_identifier(table)} AS target '
            f"JOIN continuity_selected_keys AS selected_keys ON {join} "
            f"GROUP BY {projection}"
        ).fetchall()
    history = {
        _normalized_key(tuple(row[: len(key_columns)])): tuple(
            int(value or 0) for value in row[len(key_columns) : len(key_columns) + 3]
        )
        for row in rows
    }
    if len(history) != keys.num_rows:
        raise AssertionError(
            f"Recovery artifact {table}: expected histories for {keys.num_rows} selected keys, "
            f"found {len(history)}."
        )
    return history


def _history_totals(history: dict[tuple[str | None, ...], tuple[int, int, int]]) -> tuple[int, int, int]:
    return tuple(sum(counts[index] for counts in history.values()) for index in range(3))  # type: ignore[return-value]


def _export_sample_and_change_one(
    *, source_db: Path, source, output_path: Path, sample_rows: int = SAMPLE_ROWS
) -> pa.Table:
    """Export current rows, change one STATUS, and return their unique business keys."""
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
        key_columns = [by_lower[key.lower()] for key in source.primary_key if key.lower() in by_lower]
        missing_keys = [key for key in source.primary_key if key.lower() not in by_lower]
        if missing_keys:
            raise RuntimeError(f"raw.{table} is missing configured primary-key columns: {missing_keys}")
        projection = ", ".join(_quoted_identifier(name) for name in business_columns)
        order_by = ", ".join(_quoted_identifier(name) for name in key_columns)
        non_null_keys = " AND ".join(f"{_quoted_identifier(name)} IS NOT NULL" for name in key_columns)
        arrow_table = db.execute(
            f"SELECT {projection} FROM raw.{_quoted_identifier(table)} "
            f"WHERE {_quoted_identifier(valid_to)} IS NULL AND {non_null_keys} "
            f"ORDER BY {order_by} LIMIT {int(sample_rows)}"
        ).to_arrow_table()

    if arrow_table.num_rows != sample_rows:
        raise RuntimeError(
            f"raw.{table} has only {arrow_table.num_rows} eligible current rows; "
            f"the continuity test requires {sample_rows}."
        )
    selected_keys = arrow_table.select(key_columns)
    _key_rows(selected_keys)
    status_index = arrow_table.schema.get_field_index(status_col)
    status_values = arrow_table.column(status_index).to_pylist()
    original_status = status_values[0]
    changed_status = f"{original_status or 'UNKNOWN'}__CONTINUITY_TEST__"
    if changed_status == original_status:
        changed_status = f"{changed_status}_CHANGED"
    status_values[0] = changed_status
    field = arrow_table.schema.field(status_index)
    arrow_table = arrow_table.set_column(status_index, field, pa.array(status_values, type=field.type))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_table, output_path, compression="zstd")
    print(
        f"SAMPLED {table}: {arrow_table.num_rows:,} current rows; changed STATUS for "
        f"one row while preserving its primary key; included {arrow_table.num_columns} business columns."
    )
    return selected_keys


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
        migrated_baseline = {
            source_map[name].warehouse_table.upper(): _counts(
                conn, SOURCE_SCHEMA, source_map[name].warehouse_table.upper()
            )
            for name in SOURCE_NAMES
        }
        metadata_prior = {
            table: _row_count(conn, SOURCE_SCHEMA, table)
            for table in REQUIRED_TABLES
            if table.startswith("_DLT_")
        }
        state_names = _state_pipeline_names(conn, SOURCE_SCHEMA)
        if not PIPELINE_NAMES.issubset(state_names):
            raise RuntimeError(f"Missing migrated pipeline state: {sorted(PIPELINE_NAMES - state_names)}")

        # Verify the supplied recovery DB is exactly the baseline that was migrated.
        with duckdb.connect(str(source_db), read_only=True) as db:
            for name in SOURCE_NAMES:
                table = source_map[name].warehouse_table
                expected = _duckdb_counts(db, table)
                if expected != migrated_baseline[table.upper()]:
                    raise AssertionError(
                        f"Recovery baseline mismatch for {table}: DuckDB={expected}; "
                        f"Snowflake={migrated_baseline[table.upper()]}"
                    )
            for table, count in metadata_prior.items():
                if _duckdb_row_count(db, table.lower()) != count:
                    raise AssertionError(f"Recovery metadata count mismatch for {table}.")

        print(f"MIGRATED BASELINE: {migrated_baseline}")
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
        for table, expected in migrated_baseline.items():
            actual = _counts(conn, dataset, table)
            if actual != expected:
                raise AssertionError(f"Clone mismatch for {table}: expected={expected}; actual={actual}")
        cloned_metadata = {table: _row_count(conn, dataset, table) for table in metadata_prior}
        if cloned_metadata != metadata_prior:
            raise AssertionError(f"Cloned metadata mismatch: expected={metadata_prior}; actual={cloned_metadata}")
        print(f"CLONE VERIFIED: SCD2 counts={migrated_baseline}; metadata={cloned_metadata}")

        with TemporaryDirectory(prefix="snowflake-continuity-sample-") as temp_dir:
            temp = Path(temp_dir)
            snapshots: dict[str, Path] = {}
            selected_keys: dict[str, pa.Table] = {}
            baseline_by_key: dict[str, dict[tuple[str | None, ...], tuple[int, int, int]]] = {}
            baseline_counts: dict[str, tuple[int, int, int]] = {}

            # Build bounded snapshots and keys from the exact migration recovery artifact.
            for name in SOURCE_NAMES:
                source = source_map[name]
                snapshot = temp / f"{name}.parquet"
                selected_keys[name] = _export_sample_and_change_one(
                    source_db=source_db, source=source, output_path=snapshot
                )
                snapshots[name] = snapshot

            # Restrict each clone table to all versions of the selected keys. The normal
            # loader remains untouched and receives a complete snapshot for this test set.
            for name in SOURCE_NAMES:
                source = source_map[name]
                table = source.warehouse_table.upper()
                expected_history = _duckdb_history_by_key(source_db, source, selected_keys[name])
                _restrict_clone_to_selected_keys(conn, dataset, source, selected_keys[name])
                actual_history = _snowflake_history_by_key(conn, dataset, source, selected_keys[name])
                if actual_history != expected_history:
                    raise AssertionError(
                        f"{table}: selected-key history differs between recovery artifact and "
                        "isolated Snowflake clone before load. "
                        f"Expected totals={_history_totals(expected_history)}; "
                        f"actual totals={_history_totals(actual_history)}."
                    )
                baseline_by_key[name] = actual_history
                baseline_counts[table] = _history_totals(actual_history)
                if _counts(conn, dataset, table) != baseline_counts[table]:
                    raise AssertionError(
                        f"{table}: isolated table totals include rows outside the selected keys."
                    )
                print(
                    f"SELECTED HISTORY VERIFIED {name}: keys={len(actual_history):,}; "
                    f"total/current/expired={baseline_counts[table]}"
                )

            # Release the direct SQL session while dlt opens its own destination session(s).
            conn.close()
            conn = None
            for name in SOURCE_NAMES:
                _load_sample(credentials, dataset, source_map[name], snapshots[name])

            conn = _connect(credentials)
            after_load: dict[str, dict[tuple[str | None, ...], tuple[int, int, int]]] = {}
            for name in SOURCE_NAMES:
                source = source_map[name]
                table = source.warehouse_table.upper()
                before_by_key = baseline_by_key[name]
                after_by_key = _snowflake_history_by_key(conn, dataset, source, selected_keys[name])
                key_rows = _key_rows(selected_keys[name])
                changed_key = _normalized_key(key_rows[0])
                expected_by_key = {
                    key: (
                        counts[0] + 1,
                        counts[1],
                        counts[2] + 1,
                    ) if key == changed_key else counts
                    for key, counts in before_by_key.items()
                }
                if after_by_key != expected_by_key:
                    differences = {
                        key: {"expected": expected_by_key.get(key), "actual": after_by_key.get(key)}
                        for key in set(expected_by_key) | set(after_by_key)
                        if expected_by_key.get(key) != after_by_key.get(key)
                    }
                    sample_differences = dict(list(differences.items())[:5])
                    raise AssertionError(
                        f"{table}: per-key history differs after changed snapshot; "
                        f"only key {changed_key} should gain one expired version and one total "
                        f"version. First differences={sample_differences}"
                    )
                actual_totals = _counts(conn, dataset, table)
                expected_totals = _history_totals(expected_by_key)
                if actual_totals != expected_totals:
                    raise AssertionError(
                        f"{table}: aggregate history mismatch after changed snapshot; "
                        f"expected={expected_totals}; actual={actual_totals}"
                    )
                after_load[name] = after_by_key
                print(
                    f"PASS migrated-history change detection {name}: "
                    f"keys={len(after_by_key):,}; total/current/expired={actual_totals}; "
                    "exactly one selected key gained one version."
                )

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
                source = source_map[name]
                table = source.warehouse_table.upper()
                replay_history = _snowflake_history_by_key(conn, dataset, source, selected_keys[name])
                if replay_history != after_load[name]:
                    differences = {
                        key: {"before replay": after_load[name].get(key), "after replay": replay_history.get(key)}
                        for key in set(after_load[name]) | set(replay_history)
                        if after_load[name].get(key) != replay_history.get(key)
                    }
                    raise AssertionError(
                        f"{table}: identical sample replay changed per-key history. "
                        f"First differences={dict(list(differences.items())[:5])}"
                    )
                actual_totals = _counts(conn, dataset, table)
                expected_totals = _history_totals(after_load[name])
                if actual_totals != expected_totals:
                    raise AssertionError(
                        f"{table}: identical replay aggregate changed; "
                        f"expected={expected_totals}; actual={actual_totals}"
                    )
                print(
                    f"PASS identical replay {name}: keys={len(replay_history):,}; "
                    f"total/current/expired={actual_totals}"
                )

        print(
            "SNOWFLAKE MIGRATED-HISTORY CONTINUITY PASSED: the isolated clone retained "
            "complete migrated histories for 500 selected keys per source; unchanged keys "
            "created no versions, one deliberate STATUS change created exactly one version, "
            "and an identical replay changed no per-key history."
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
