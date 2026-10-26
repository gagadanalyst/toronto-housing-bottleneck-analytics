"""Verify the real dlt pipeline can continue from migrated Snowflake SCD2 history.

The workflow clones TORONTO.RAW into a uniquely named, temporary schema,
performs a live Active/Cleared extract and dlt load into that clone, then replays
the exact extracted snapshots once. TORONTO.RAW is source-only and is never a
write destination during the test.
"""

from __future__ import annotations

import base64
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_der_private_key,
    load_pem_private_key,
)
from toronto_housing_ingestion.config import load_config

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


def _safe_schema(value: str) -> str:
    """Accept only this test's per-run destination schema convention."""
    normalized = value.upper()
    if not re.fullmatch(r"SNOWFLAKE_CONTINUITY_[A-Z0-9_]+", normalized):
        raise ValueError("Refusing to run outside SNOWFLAKE_CONTINUITY_<run-id>.")
    return normalized


def _decode_private_key(encoded_key: str, passphrase: str | None) -> bytes:
    """Decode dlt-supported PEM/DER key representations to connector-ready DER.

    dlt accepts plain PEM, base64-encoded PEM, and base64-encoded DER. Query
    strings also require URL-encoding ``+`` characters; this tries a recovery
    candidate for common URLs where ``+`` was decoded as a space.
    """
    password = passphrase.encode("utf-8") if passphrase else None
    candidates: list[str] = [encoded_key]
    if "-----BEGIN" not in encoded_key:
        # parse_qs turns an unescaped '+' into a space. Try both a compact
        # candidate (for line-wrapped base64) and a restored-plus candidate.
        compact = "".join(encoded_key.split())
        restored_plus = encoded_key.replace(" ", "+")
        candidates.extend((restored_plus, compact))

    attempted: set[bytes] = set()
    for candidate in candidates:
        raw = candidate.encode("utf-8")
        if raw not in attempted:
            attempted.add(raw)
            for loader in (load_pem_private_key, load_der_private_key):
                try:
                    private_key = loader(raw, password=password)
                    return private_key.private_bytes(
                        Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
                    )
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
                private_key = loader(decoded, password=password)
                return private_key.private_bytes(
                    Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
                )
            except (TypeError, ValueError):
                pass

    raise ValueError(
        "Unable to decode the Snowflake private_key. Supported formats are "
        "plain PEM, base64-encoded PEM, and base64-encoded DER; verify URL "
        "encoding of the private_key query parameter and passphrase."
    )


def _connect(uri: str):
    """Connect using the key-pair Snowflake dlt URI from GitHub Secrets."""
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

    passphrase = query.get("private_key_passphrase", [None])[0]
    key_der = _decode_private_key(encoded_key, passphrase)
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
        raise RuntimeError(
            f"{schema}._DLT_PIPELINE_STATE has no PIPELINE_NAME column; "
            "cannot verify migrated state."
        )
    cursor = conn.cursor()
    try:
        return {
            str(row[0]).upper()
            for row in cursor.execute(
                f"SELECT DISTINCT PIPELINE_NAME "
                f"FROM {DATABASE}.{schema}._DLT_PIPELINE_STATE "
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
            f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) "
            f"FROM {DATABASE}.{schema}.{table}"
        ).fetchone()
    finally:
        cursor.close()
    total, current, expired = (int(value or 0) for value in row)
    if total != current + expired:
        raise AssertionError(
            f"{schema}.{table}: total {total} != current {current} + expired {expired}."
        )
    return total, current, expired


def _row_count(conn, schema: str, table: str) -> int:
    if table not in REQUIRED_TABLES:
        raise ValueError(f"Unexpected metadata table: {table}")
    cursor = conn.cursor()
    try:
        return int(
            cursor.execute(
                f"SELECT COUNT(*) FROM {DATABASE}.{schema}.{table}"
            ).fetchone()[0]
        )
    finally:
        cursor.close()


def _run_cli(*arguments: str) -> None:
    command = [sys.executable, "-m", "toronto_housing_ingestion.ingestion", *arguments]
    print("\n$ " + " ".join(command), flush=True)
    subprocess.run(command, check=True)


def main() -> None:
    """Clone migrated history, load live snapshots, and check idempotent replay."""
    credentials = os.environ.get("TORONTO_WAREHOUSE_CREDENTIALS", "")
    if os.environ.get("TORONTO_WAREHOUSE_KIND", "").lower() != "snowflake":
        raise SystemExit("Set TORONTO_WAREHOUSE_KIND=snowflake for this test.")
    if not credentials:
        raise SystemExit("TORONTO_WAREHOUSE_CREDENTIALS is not configured.")
    dataset = _safe_schema(os.environ.get("TORONTO_WAREHOUSE_DATASET", ""))

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
                raise RuntimeError(
                    "Unexpected Snowflake database or role; stopping before cloning."
                )
        finally:
            cursor.close()

        available = _table_inventory(conn, SOURCE_SCHEMA)
        missing = set(REQUIRED_TABLES) - available
        if missing:
            raise RuntimeError(
                f"TORONTO.RAW is missing migrated tables: {sorted(missing)}. "
                "Run the Snowflake History Migration workflow first."
            )

        prior = {table: _counts(conn, SOURCE_SCHEMA, table) for table in SCD2_TABLES}
        metadata_prior = {
            table: _row_count(conn, SOURCE_SCHEMA, table)
            for table in REQUIRED_TABLES
            if table.startswith("_DLT_")
        }
        state_names = _state_pipeline_names(conn, SOURCE_SCHEMA)
        missing_states = PIPELINE_NAMES - state_names
        if missing_states:
            raise RuntimeError(
                "TORONTO.RAW is missing migrated dlt pipeline state for: "
                f"{sorted(missing_states)}."
            )
        if any(values[0] == 0 or values[1] == 0 for values in prior.values()):
            raise RuntimeError(f"TORONTO.RAW SCD2 source tables look empty: {prior}")
        if metadata_prior.get("_DLT_PIPELINE_STATE", 0) == 0:
            raise RuntimeError("TORONTO.RAW has no migrated _DLT_PIPELINE_STATE rows.")
        for table, values in prior.items():
            print(
                f"MIGRATED BASELINE {table}: total={values[0]:,}, "
                f"current={values[1]:,}, expired={values[2]:,}"
            )
        print(f"MIGRATED METADATA ROWS: {metadata_prior}")
        print(f"MIGRATED PIPELINE STATES: {sorted(state_names)}")

        # Each run starts from a fresh clone. The allowlist above ensures this
        # cleanup can never target TORONTO.RAW or any unrelated schema.
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{dataset} CASCADE")
            cursor.execute(
                f"CREATE SCHEMA {DATABASE}.{dataset} CLONE {DATABASE}.{SOURCE_SCHEMA}"
            )
            schema_created = True
        finally:
            cursor.close()
        print(f"CLONED {DATABASE}.{SOURCE_SCHEMA} to isolated {DATABASE}.{dataset}.")

        cloned_states = _state_pipeline_names(conn, dataset)
        missing_cloned = PIPELINE_NAMES - cloned_states
        if missing_cloned:
            raise RuntimeError(
                f"Clone is missing pipeline state: {sorted(missing_cloned)}."
            )

        # Verify the clone is identical to the migrated baseline before letting
        # the application write to it. This prevents a bad clone from being
        # masked by newly ingested rows.
        for table, expected in prior.items():
            actual = _counts(conn, dataset, table)
            if actual != expected:
                raise AssertionError(
                    f"Cloned {table} does not match TORONTO.RAW. "
                    f"Expected={expected}; cloned={actual}."
                )
        cloned_metadata = {
            table: _row_count(conn, dataset, table) for table in metadata_prior
        }
        if cloned_metadata != metadata_prior:
            raise AssertionError(
                "Cloned dlt metadata row counts do not match TORONTO.RAW. "
                f"Expected={metadata_prior}; cloned={cloned_metadata}."
            )
        print(f"CLONE VERIFIED: SCD2 counts={prior}; metadata rows={cloned_metadata}")

        app = load_config(Path("open_data_config.yml"))
        sources = {source.name: source for source in app.sources}
        source_args = tuple(arg for name in SOURCE_NAMES for arg in ("--source", name))

        # Same pipeline names as the migrated state; only the dataset target is
        # redirected to the isolated clone. This is the continuity check.
        # Close the control connection during the long extraction/load subprocess
        # so it does not hold an idle Snowflake session for the whole extraction.
        conn.close()
        conn = None
        _run_cli("--full-refresh", *source_args)
        conn = _connect(credentials)

        after_live: dict[str, tuple[int, int, int]] = {}
        for name, table in zip(SOURCE_NAMES, SCD2_TABLES, strict=True):
            after = _counts(conn, dataset, table)
            before = prior[table]
            if after[0] < before[0] or after[2] < before[2]:
                raise AssertionError(
                    f"{table}: migrated history was lost. Before={before}; after live load={after}."
                )
            # If existing row hashes/state were not restored, nearly all current
            # rows could be re-versioned. This threshold catches that failure.
            max_growth = max(10_000, int(before[1] * 0.10))
            if after[0] - before[0] > max_growth:
                raise AssertionError(
                    f"{table}: history grew by {after[0] - before[0]:,} rows, above "
                    f"the safety threshold of {max_growth:,}; likely state/hash mismatch."
                )
            if after[1] == 0:
                raise AssertionError(f"{table}: there are no current rows after continuation.")
            after_live[table] = after
            print(
                f"PASS migrated-state continuation {name}: before={before}; "
                f"after live load={after}; history added={after[0] - before[0]:,}"
            )

        metadata_after = {
            table: _row_count(conn, dataset, table) for table in metadata_prior
        }
        for table, count in metadata_prior.items():
            if metadata_after.get(table, 0) < count:
                raise AssertionError(
                    f"{table}: metadata row count decreased from {count} to "
                    f"{metadata_after.get(table, 0)}."
                )
        state_names_after = _state_pipeline_names(conn, dataset)
        if not PIPELINE_NAMES.issubset(state_names_after):
            raise AssertionError(
                f"Pipeline state records missing after continuation: {sorted(PIPELINE_NAMES - state_names_after)}."
            )
        print(f"METADATA AFTER CONTINUATION: {metadata_after}")

        # The separate Snowflake Live Ingestion Smoke workflow already verifies
        # two identical replays on a clean target. For migrated-history
        # continuity, one replay is sufficient to prove this target stays stable
        # after the live continuation load.
        baseline = dict(after_live)
        conn.close()
        conn = None
        _run_cli("--local-load", *source_args)
        conn = _connect(credentials)
        for name, table in zip(SOURCE_NAMES, SCD2_TABLES, strict=True):
            current = _counts(conn, dataset, table)
            if current != baseline[table]:
                raise AssertionError(
                    f"{table}: identical replay changed counts. "
                    f"Baseline={baseline[table]}, after={current}."
                )
            print(
                f"PASS {name} identical replay: total={current[0]:,}, "
                f"current={current[1]:,}, expired={current[2]:,}; no extra versions."
            )

        print(
            "SNOWFLAKE MIGRATED-HISTORY CONTINUITY PASSED: cloned migrated "
            "history accepted a live extraction, preserved prior versions, "
            "and stayed stable across one identical replay."
        )
    finally:
        if schema_created:
            try:
                # If the CLI subprocess failed while the control connection was
                # closed, reconnect only for best-effort cleanup.
                if conn is None:
                    conn = _connect(credentials)
                cursor = conn.cursor()
                try:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{dataset} CASCADE")
                finally:
                    cursor.close()
                print(f"Cleaned up isolated schema {DATABASE}.{dataset}.", flush=True)
            except Exception as exc:
                print(
                    f"WARNING: unable to clean up isolated schema {DATABASE}.{dataset}: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
