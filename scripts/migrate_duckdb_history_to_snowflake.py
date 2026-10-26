"""Copy DuckDB SCD2 history and dlt metadata into Snowflake without replaying it."""

from __future__ import annotations

import argparse
import base64
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import duckdb
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from snowflake.connector.pandas_tools import write_pandas

TABLES = (
    "raw_active_permits",
    "raw_cleared_permits",
    "_dlt_loads",
    "_dlt_version",
    "_dlt_pipeline_state",
)
PERMIT_TABLES = {"raw_active_permits", "raw_cleared_permits"}
DATABASE = "TORONTO"
TARGET_SCHEMA = "RAW"
STAGE_SCHEMA = "MIGRATION_STAGE"


def connect_from_dlt_uri(uri: str):
    """Create a Snowflake connection from the dlt URI in GitHub Secrets."""
    parts = urlsplit(uri)
    query = parse_qs(parts.query)

    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("Expected a Snowflake dlt connection URI.")

    encoded_key = query.get("private_key", [None])[0]
    if not encoded_key:
        raise ValueError("The connection URI must use private-key authentication.")

    pem = base64.b64decode(encoded_key)
    private_key = load_pem_private_key(pem, password=None)
    key_der = private_key.private_bytes(
        Encoding.DER,
        PrivateFormat.PKCS8,
        NoEncryption(),
    )

    database = unquote(parts.path.strip("/").split("/")[0])
    if database.upper() != DATABASE:
        raise ValueError(f"Expected database {DATABASE}; got {database!r}.")

    return snowflake.connector.connect(
        account=parts.hostname,
        user=unquote(parts.username),
        private_key=key_der,
        database=DATABASE,
        schema=TARGET_SCHEMA,
        warehouse=query["warehouse"][0],
        role=query["role"][0],
        application="TORONTO_HOUSING_HISTORY_MIGRATION",
    )


def source_counts(db: duckdb.DuckDBPyConnection) -> dict[str, dict[str, int]]:
    """Count source rows and current SCD2 rows in DuckDB."""
    result = {}
    for table in TABLES:
        total = int(
            db.execute(f'SELECT COUNT(*) FROM raw."{table}"').fetchone()[0]
        )
        values = {"total": total}
        if table in PERMIT_TABLES:
            values["current"] = int(
                db.execute(
                    f'SELECT COUNT(*) FROM raw."{table}" '
                    "WHERE _dlt_valid_to IS NULL"
                ).fetchone()[0]
            )
        result[table] = values
    return result


def snowflake_counts(conn, schema: str) -> dict[str, dict[str, int]]:
    """Count rows in a Snowflake schema."""
    result = {}
    cursor = conn.cursor()
    try:
        for table in TABLES:
            qualified = f"{DATABASE}.{schema}.{table.upper()}"
            total = int(
                cursor.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            )
            values = {"total": total}
            if table in PERMIT_TABLES:
                values["current"] = int(
                    cursor.execute(
                        f"SELECT COUNT_IF(_DLT_VALID_TO IS NULL) "
                        f"FROM {qualified}"
                    ).fetchone()[0]
                )
            result[table] = values
    finally:
        cursor.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", required=True, type=Path)
    args = parser.parse_args()

    uri = __import__("os").environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    if not uri:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is missing.")
    if not args.source_db.is_file():
        raise SystemExit(f"Source database not found: {args.source_db}")

    with duckdb.connect(str(args.source_db), read_only=True) as source:
        expected = source_counts(source)
        for table, values in expected.items():
            print(f"Source raw.{table}: {values}")

        conn = connect_from_dlt_uri(uri)
        try:
            cursor = conn.cursor()
            try:
                database, schema, role = cursor.execute(
                    "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()"
                ).fetchone()
                print(f"Connected to {database}.{schema} using role {role}")
                if database.upper() != DATABASE or role.upper() != "TORONTO_INGEST_ROLE":
                    raise RuntimeError(
                        "Unexpected database or role; stopping before migration."
                    )

                names = ", ".join(f"'{t.upper()}'" for t in TABLES)
                existing = {
                    row[0]
                    for row in cursor.execute(
                        "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}' "
                        f"AND TABLE_NAME IN ({names})"
                    ).fetchall()
                }
            finally:
                cursor.close()

            required = {t.upper() for t in TABLES}
            if existing:
                if existing != required:
                    raise RuntimeError(
                        "A partial migration exists in TORONTO.RAW. "
                        f"Found {sorted(existing)}; inspect it before retrying."
                    )
                actual = snowflake_counts(conn, TARGET_SCHEMA)
                if actual != expected:
                    raise RuntimeError(
                        "Migration tables exist, but counts differ. "
                        f"Expected {expected}; found {actual}."
                    )
                print("Migration already exists and row counts match; no changes made.")
                return

            # Build an isolated staging schema. A retry cleans up only this
            # migration's staging schema, never the final RAW schema.
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"DROP SCHEMA IF EXISTS {DATABASE}.{STAGE_SCHEMA} CASCADE"
                )
                cursor.execute(
                    f"CREATE SCHEMA {DATABASE}.{STAGE_SCHEMA}"
                )
            finally:
                cursor.close()

            # Load completed load metadata first, then source history, then state.
            order = (
                "_dlt_loads",
                "_dlt_version",
                "raw_active_permits",
                "raw_cleared_permits",
                "_dlt_pipeline_state",
            )
            for table in order:
                frame = source.execute(f'SELECT * FROM raw."{table}"').fetchdf()
                frame.columns = [str(column).upper() for column in frame.columns]

                success, chunks, rows, _ = write_pandas(
                    conn,
                    frame,
                    table_name=table.upper(),
                    database=DATABASE,
                    schema=STAGE_SCHEMA,
                    auto_create_table=True,
                    overwrite=False,
                    quote_identifiers=True,
                    use_logical_type=True,
                )
                if not success or int(rows) != len(frame):
                    raise RuntimeError(
                        f"Staging load failed for {table}: "
                        f"success={success}, loaded={rows}, expected={len(frame)}."
                    )
                print(f"Staged {table}: {rows:,} rows across {chunks} chunks")

            staged = snowflake_counts(conn, STAGE_SCHEMA)
            if staged != expected:
                raise RuntimeError(
                    f"Staging count mismatch. Expected {expected}; found {staged}."
                )
            print("All staged row counts and current-version counts match.")

            # Publish only after all five staging tables have passed verification.
            cursor = conn.cursor()
            try:
                for table in TABLES:
                    cursor.execute(
                        f"CREATE TABLE {DATABASE}.{TARGET_SCHEMA}.{table.upper()} "
                        f"CLONE {DATABASE}.{STAGE_SCHEMA}.{table.upper()}"
                    )
            finally:
                cursor.close()

            final_counts = snowflake_counts(conn, TARGET_SCHEMA)
            if final_counts != expected:
                raise RuntimeError(
                    f"Final count mismatch. Expected {expected}; "
                    f"found {final_counts}."
                )

            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"DROP SCHEMA {DATABASE}.{STAGE_SCHEMA} CASCADE"
                )
            finally:
                cursor.close()

            print("SNOWFLAKE HISTORY MIGRATION PASSED")
            print("Historical rows, current-version counts and dlt metadata matched.")
            print("This is a migration test baseline, not a claim of current-source parity.")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
