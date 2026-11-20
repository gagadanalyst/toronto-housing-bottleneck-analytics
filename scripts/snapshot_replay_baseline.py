"""Capture and verify the production baseline used by a staged Mode 1 replay.

DuckDB uses a cryptographic file digest. Snowflake uses stable aggregate signatures
for both canonical SCD2 tables and the persistent snapshot ledger. The workflow
captures after candidate preparation and verifies immediately before promotion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def duckdb_signature(database: Path) -> dict[str, Any]:
    path = database.resolve()
    if not path.is_file() or path.stat().st_size <= 0:
        raise FileNotFoundError(
            f"Canonical DuckDB baseline is missing or empty: {path}"
        )
    return {
        "backend": "duckdb",
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _snowflake_connect(uri: str):
    import snowflake.connector

    from scripts.replay_targets import SnowflakeReplayTarget

    options, key_bytes, passphrase = SnowflakeReplayTarget._parse_credentials(uri)
    temporary = tempfile.TemporaryDirectory(prefix="toronto-replay-baseline-")
    key_path = Path(temporary.name) / "snowflake_key.p8"
    key_path.write_bytes(key_bytes)
    key_path.chmod(0o600)
    # Match the replay target's account/role/database/warehouse guard and pin the
    # session schema explicitly to canonical RAW rather than a user default.
    connect_options = {**options, "private_key_file": str(key_path), "schema": "RAW"}
    if passphrase:
        connect_options["private_key_file_pwd"] = passphrase
    try:
        connection = snowflake.connector.connect(**connect_options)
    except Exception:
        temporary.cleanup()
        raise
    return connection, temporary


def snowflake_signature(uri: str) -> dict[str, Any]:
    """Fingerprint canonical RAW contents using deterministic Snowflake aggregates."""
    from scripts.snowflake_operations import require_snowflake_operations_enabled

    require_snowflake_operations_enabled("Snowflake baseline capture")
    connection, temporary = _snowflake_connect(uri)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), "
            "CURRENT_WAREHOUSE(), CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
        )
        role, database, schema, warehouse, organization, account = cursor.fetchone()
        from scripts.replay_targets import _validate_snowflake_account_context

        try:
            _validate_snowflake_account_context(
                role, database, warehouse, organization, account
            )
        except ValueError as exc:
            raise RuntimeError(
                "Snowflake baseline capture requires the canonical ingest context; "
                f"observed role/database/warehouse/organization/account="
                f"{role}/{database}/{warehouse}/{organization}/{account}."
            ) from exc
        if str(schema).upper() != "RAW":
            raise RuntimeError(
                "Snowflake baseline capture requires TORONTO.RAW context; "
                f"observed schema={schema!r}."
            )

        tables: dict[str, dict[str, Any]] = {}
        for table in ("RAW_ACTIVE_PERMITS", "RAW_CLEARED_PERMITS"):
            cursor.execute(
                f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
                f"COUNT_IF(_DLT_VALID_TO IS NOT NULL), "
                "HASH_AGG(HASH(PERMIT_NUM, REVISION_NUM, PERMIT_TYPE, ROW_HASH, "
                f"_DLT_VALID_FROM, _DLT_VALID_TO)) FROM TORONTO.RAW.{table}"
            )
            total, current, expired, digest = cursor.fetchone()
            values = {
                "total": int(total or 0),
                "current": int(current or 0),
                "expired": int(expired or 0),
                "hash_agg": None if digest is None else str(digest),
            }
            if values["total"] <= 0 or values["current"] <= 0 or digest is None:
                raise RuntimeError(
                    f"TORONTO.RAW.{table} cannot provide a complete baseline signature: {values}."
                )
            tables[table] = values

        # Fingerprint the complete durable snapshot-ledger identity and lifecycle,
        # not just the snapshot/source/status triple. A provenance backfill or a
        # changed source identity must invalidate a candidate prepared from the
        # older RAW baseline.
        cursor.execute(
            "SELECT COUNT(*), COUNT(DISTINCT SNAPSHOT_ID), "
            "HASH_AGG(HASH(SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT, "
            "EXTRACTION_STARTED_AT_UTC, SOURCE_NAME, STATUS, SOURCE_COMMIT, "
            "RELEASE_TAG, ASSET_NAME, ERROR, CREATED_AT, UPDATED_AT, "
            "QUARANTINED_ROW_COUNT, QUARANTINED_BAD_ROW_COUNT, "
            "QUARANTINED_BUSINESS_KEY_COUNT, QUARANTINE_SHA256)) "
            "FROM TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"
        )
        ledger_rows, snapshots, ledger_digest = cursor.fetchone()
        ledger = {
            "rows": int(ledger_rows or 0),
            "snapshots": int(snapshots or 0),
            "hash_agg": None if ledger_digest is None else str(ledger_digest),
        }
        if ledger["rows"] <= 0 or ledger["hash_agg"] is None:
            raise RuntimeError(
                "TORONTO.RAW snapshot ledger cannot provide a baseline signature."
            )

        # DLT's persisted pipeline state is part of the production baseline. If
        # another writer advances a pipeline while this candidate is loading, a
        # raw-row/ledger-only fingerprint could miss the changed checkpoint and
        # later promote stale DLT state over the newer production state.
        cursor.execute(
            "SELECT COUNT(*), COUNT(DISTINCT PIPELINE_NAME), "
            "HASH_AGG(HASH(PIPELINE_NAME, STATE, VERSION, ENGINE_VERSION, "
            "VERSION_HASH, _DLT_LOAD_ID)) "
            "FROM TORONTO.RAW._DLT_PIPELINE_STATE"
        )
        state_rows, state_pipelines, state_digest = cursor.fetchone()
        pipeline_state = {
            "rows": int(state_rows or 0),
            "pipelines": int(state_pipelines or 0),
            "hash_agg": None if state_digest is None else str(state_digest),
        }
        if (
            pipeline_state["rows"] <= 0
            or pipeline_state["pipelines"] != 2
            or pipeline_state["hash_agg"] is None
        ):
            raise RuntimeError(
                "TORONTO.RAW DLT pipeline state cannot provide a complete baseline signature: "
                f"{pipeline_state}."
            )
        return {
            "backend": "snowflake",
            "account": f"{organization}-{account}".lower(),
            "organization": str(organization),
            "account_locator": str(account),
            "database": str(database),
            "schema": str(schema),
            "role": str(role),
            "warehouse": str(warehouse),
            "tables": tables,
            "snapshot_ledger": ledger,
            "dlt_pipeline_state": pipeline_state,
        }
    finally:
        cursor.close()
        connection.close()
        temporary.cleanup()


def capture(
    backend: str, *, database: Path | None, credentials: str | None
) -> dict[str, Any]:
    if backend == "duckdb":
        if database is None:
            raise ValueError("--database is required for DuckDB baseline capture.")
        return duckdb_signature(database)
    if backend == "snowflake":
        if not credentials:
            raise ValueError(
                "SNOWFLAKE_DLT_CONNECTION is required for Snowflake baseline capture."
            )
        return snowflake_signature(credentials)
    raise ValueError(f"Unsupported baseline backend: {backend!r}.")


def verify(expected_path: Path, observed: dict[str, Any]) -> None:
    try:
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot read captured production baseline {expected_path}: {exc}"
        ) from exc
    if not isinstance(expected, dict) or expected != observed:
        raise RuntimeError(
            "Production baseline changed after the incremental candidate was prepared; "
            "refusing promotion. "
            f"expected={json.dumps(expected, sort_keys=True)}, "
            f"observed={json.dumps(observed, sort_keys=True)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("duckdb", "snowflake"), required=True)
    parser.add_argument(
        "--database", type=Path, default=Path("data/warehouse/toronto.duckdb")
    )
    parser.add_argument("--expected", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--snowflake-credentials", default=os.getenv("SNOWFLAKE_DLT_CONNECTION")
    )
    args = parser.parse_args()
    try:
        observed = capture(
            args.backend,
            database=args.database if args.backend == "duckdb" else None,
            credentials=args.snowflake_credentials,
        )
        if args.expected:
            verify(args.expected, observed)
            print(f"PASS: {args.backend} production baseline is unchanged.")
        else:
            if args.output is None:
                parser.error("--output is required when capturing a baseline.")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(observed, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            print(f"Captured {args.backend} production baseline: {args.output}")
        return 0
    except Exception as exc:
        print(f"BASELINE CHECK FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
