"""Clone canonical Snowflake RAW and prepare an isolated SCD2 load target."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import snowflake.connector

try:
    from snowflake_candidate_utils import decode_state, rebase_pipeline_state
except ImportError:
    from scripts.snowflake_candidate_utils import decode_state, rebase_pipeline_state

TABLES = {
    "active_permits": "RAW_ACTIVE_PERMITS",
    "cleared_permits": "RAW_CLEARED_PERMITS",
}


def _connection_options(uri: str, key_path: Path) -> dict[str, str]:
    parts = urlsplit(uri)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is not a valid Snowflake URI.")
    encoded = params.get("private_key", [""])[0].replace(" ", "+")
    key = base64.b64decode(encoded, validate=True)
    if not key:
        raise ValueError("Snowflake private key is empty.")
    key_path.write_bytes(key)
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


def _recover_interrupted_raw_swap(cursor) -> int:
    """Finish a prior SWAP whose DLT-state rebase was interrupted."""
    cursor.execute("SELECT PIPELINE_NAME, STATE FROM TORONTO.RAW._DLT_PIPELINE_STATE")
    rows = cursor.fetchall()
    staged_datasets = set()
    for pipeline_name, state_value in rows:
        payload = decode_state(state_value, str(pipeline_name).lower())
        dataset = str(payload.get("dataset_name", "")).upper()
        if dataset == "RAW":
            continue
        if not re.fullmatch(r"RAW_STAGE_[0-9]+_[0-9]+", dataset):
            raise RuntimeError(
                f"Canonical RAW DLT state points to unexpected dataset {dataset!r}."
            )
        staged_datasets.add(dataset)
    if not staged_datasets:
        return 0
    if len(staged_datasets) != 1:
        raise RuntimeError(
            f"Canonical RAW contains mixed staged DLT dataset names: {sorted(staged_datasets)}."
        )
    previous_stage = next(iter(staged_datasets))
    cursor.execute(
        "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
        "WHERE SCHEMA_NAME = %s",
        (previous_stage,),
    )
    if int(cursor.fetchone()[0] or 0) != 1:
        raise RuntimeError(
            f"Canonical RAW DLT state references {previous_stage}, but that retained "
            "schema does not exist; refusing to guess at recovery."
        )
    updated = rebase_pipeline_state(cursor, "RAW", previous_stage, "RAW")
    cursor.execute(
        "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'TORONTO_CANDIDATE_CONTROL'"
    )
    if int(cursor.fetchone()[0] or 0):
        cursor.execute(
            "UPDATE TORONTO.RAW.TORONTO_CANDIDATE_CONTROL "
            "SET PROMOTION_STATE = 'PROMOTED', UPDATED_AT = CURRENT_TIMESTAMP() "
            "WHERE PROMOTION_STATE != 'PROMOTED'"
        )
    print(
        f"Recovered interrupted RAW schema swap by rebasing {updated} DLT state row(s) "
        f"from {previous_stage} to RAW."
    )
    return updated


def prepare(uri: str, run_id: str, attempt: str, github_env: Path | None) -> str:
    if not re.fullmatch(r"[0-9]+", run_id) or not re.fullmatch(r"[0-9]+", attempt):
        raise ValueError("GitHub run ID and attempt must contain digits only.")
    stage = f"RAW_STAGE_{run_id}_{attempt}"
    with tempfile.TemporaryDirectory(prefix="toronto-raw-stage-") as temp:
        options = _connection_options(uri, Path(temp) / "snowflake_key.p8")
        connection = snowflake.connector.connect(**options)
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            role, database, schema = cursor.fetchone()
            if (str(role).upper(), str(database).upper(), str(schema).upper()) != (
                "TORONTO_INGEST_ROLE",
                "TORONTO",
                "RAW",
            ):
                raise RuntimeError(
                    f"Unexpected Snowflake context: {role}/{database}/{schema}."
                )
            _recover_interrupted_raw_swap(cursor)
            cursor.execute(
                "SELECT SCHEMA_NAME FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (stage,),
            )
            if cursor.fetchone():
                raise RuntimeError(
                    f"Candidate schema TORONTO.{stage} already exists. Re-run the "
                    "workflow to get a new run/attempt candidate; "
                    "never reuse a stale stage."
                )
            counts: dict[str, int] = {}
            for source_name, table in TABLES.items():
                cursor.execute(
                    f"SELECT COUNT_IF(_DLT_VALID_TO IS NULL) FROM TORONTO.RAW.{table}"
                )
                count = int(cursor.fetchone()[0] or 0)
                if count <= 0:
                    raise RuntimeError(f"TORONTO.RAW.{table} has no current rows.")
                counts[source_name] = count
            cursor.execute(
                "SELECT SNAPSHOT_ID, SOURCE_NAME, STATUS "
                "FROM TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"
            )
            baseline_rows = cursor.fetchall()
            baseline_statuses: dict[str, dict[str, str]] = {}
            for snapshot_id, source_name, status in baseline_rows:
                baseline_statuses.setdefault(str(snapshot_id), {})[str(source_name)] = (
                    str(status).lower()
                )
            baseline_snapshot_ids = sorted(
                snapshot_id
                for snapshot_id, statuses in baseline_statuses.items()
                if set(statuses) == set(TABLES)
                and all(statuses[name] == "succeeded" for name in TABLES)
            )
            cursor.execute(f'CREATE SCHEMA TORONTO."{stage}" CLONE TORONTO.RAW')
            quarantine_ddl = f"""CREATE TABLE IF NOT EXISTS TORONTO."{stage}".TORONTO_SCD2_QUARANTINE (
                QUARANTINE_ID VARCHAR NOT NULL,
                SNAPSHOT_ID VARCHAR NOT NULL,
                SOURCE_NAME VARCHAR NOT NULL,
                SOURCE_RUN_ID VARCHAR NOT NULL,
                SOURCE_RUN_ATTEMPT NUMBER(10,0) NOT NULL,
                EXTRACTED_AT_UTC TIMESTAMP_TZ,
                SOURCE_COMMIT VARCHAR,
                RELEASE_TAG VARCHAR,
                ASSET_NAME VARCHAR,
                PARQUET_SHA256 VARCHAR,
                ROW_ORDINAL NUMBER(38,0) NOT NULL,
                BUSINESS_KEY_JSON VARCHAR,
                RAW_RECORD_JSON VARCHAR NOT NULL,
                ROW_HASH VARCHAR NOT NULL,
                REASON VARCHAR NOT NULL,
                DETECTED_AT_UTC TIMESTAMP_TZ NOT NULL
            )"""
            cursor.execute(quarantine_ddl)
            cursor.execute(
                f'''CREATE TABLE IF NOT EXISTS TORONTO."{stage}".TORONTO_CANDIDATE_CONTROL (
                    GENERATION_ID VARCHAR NOT NULL,
                    PROMOTION_STATE VARCHAR NOT NULL,
                    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )'''
            )
            cursor.execute(f'DELETE FROM TORONTO."{stage}".TORONTO_CANDIDATE_CONTROL')
            cursor.execute(
                f'''INSERT INTO TORONTO."{stage}".TORONTO_CANDIDATE_CONTROL
                    (GENERATION_ID, PROMOTION_STATE) VALUES (%s, 'READY')''',
                (f"{run_id}:{attempt}",),
            )
            cursor.execute(
                f'ALTER TABLE TORONTO."{stage}".TORONTO_SNAPSHOT_LEDGER '
                "ADD COLUMN IF NOT EXISTS QUARANTINED_ROW_COUNT NUMBER(38,0) DEFAULT 0"
            )
            cursor.execute(
                f'ALTER TABLE TORONTO."{stage}".TORONTO_SNAPSHOT_LEDGER '
                "ADD COLUMN IF NOT EXISTS "
                "QUARANTINED_BAD_ROW_COUNT NUMBER(38,0) DEFAULT 0"
            )
            cursor.execute(
                f'ALTER TABLE TORONTO."{stage}".TORONTO_SNAPSHOT_LEDGER '
                "ADD COLUMN IF NOT EXISTS "
                "QUARANTINED_BUSINESS_KEY_COUNT NUMBER(38,0) DEFAULT 0"
            )
            cursor.execute(
                f'ALTER TABLE TORONTO."{stage}".TORONTO_SNAPSHOT_LEDGER '
                "ADD COLUMN IF NOT EXISTS QUARANTINE_SHA256 VARCHAR"
            )
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (stage,),
            )
            owner_row = cursor.fetchone()
            if not owner_row or str(owner_row[0]).upper() != "TORONTO_INGEST_ROLE":
                raise RuntimeError(
                    f"TORONTO_INGEST_ROLE must own the RAW candidate schema; observed={owner_row}."
                )
            cursor.execute(
                f'GRANT USAGE ON SCHEMA TORONTO."{stage}" TO ROLE TORONTO_DBT_ROLE'
            )
            for table in (*TABLES.values(), "_DLT_LOADS"):
                cursor.execute(
                    f'GRANT SELECT ON TABLE TORONTO."{stage}".{table} '
                    "TO ROLE TORONTO_DBT_ROLE"
                )
            updated = rebase_pipeline_state(cursor, stage, "RAW", stage)
            connection.commit()
            print(
                f"Prepared TORONTO.{stage} as a clone of TORONTO.RAW; "
                f"rebased {updated} DLT state rows; baseline current counts={counts}."
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()
    values = {
        "TORONTO_WAREHOUSE_DATASET": stage,
        "TORONTO_SNOWFLAKE_STAGE_SCHEMA": stage,
        "TORONTO_SNOWFLAKE_STAGE_GENERATION": f"{run_id}:{attempt}",
        "TORONTO_QUARANTINE_BASELINE_COUNTS": json.dumps(counts, sort_keys=True),
        "TORONTO_QUARANTINE_BASELINE_SNAPSHOT_IDS": json.dumps(
            baseline_snapshot_ids, separators=(",", ":")
        ),
    }
    if github_env:
        with github_env.open("a", encoding="utf-8") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    print(f"Snowflake quarantine denominator baseline: {counts}")
    return stage


def drop_unused_candidate(uri: str, run_id: str, attempt: str) -> bool:
    """Drop only this run's untouched READY candidate after a no-op backlog scan."""
    if not re.fullmatch(r"[0-9]+", run_id) or not re.fullmatch(r"[0-9]+", attempt):
        raise ValueError("GitHub run ID and attempt must contain digits only.")
    stage = f"RAW_STAGE_{run_id}_{attempt}"
    with tempfile.TemporaryDirectory(prefix="toronto-raw-stage-cleanup-") as temp:
        options = _connection_options(uri, Path(temp) / "snowflake_key.p8")
        connection = snowflake.connector.connect(**options)
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
            )
            role, database, schema = cursor.fetchone()
            if (str(role).upper(), str(database).upper(), str(schema).upper()) != (
                "TORONTO_INGEST_ROLE",
                "TORONTO",
                "RAW",
            ):
                raise RuntimeError(
                    f"Unexpected Snowflake context for candidate cleanup: "
                    f"{role}/{database}/{schema}."
                )
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = %s",
                (stage,),
            )
            owner = cursor.fetchone()
            if owner is None:
                print(f"No unused candidate schema TORONTO.{stage} remains.")
                return False
            if str(owner[0]).upper() != "TORONTO_INGEST_ROLE":
                raise RuntimeError(
                    f"Refusing to drop TORONTO.{stage}: unexpected owner {owner[0]!r}."
                )
            cursor.execute(
                f"SELECT GENERATION_ID, PROMOTION_STATE "
                f'FROM TORONTO."{stage}".TORONTO_CANDIDATE_CONTROL'
            )
            rows = cursor.fetchall()
            if rows != [(f"{run_id}:{attempt}", "READY")]:
                raise RuntimeError(
                    f"Refusing to drop TORONTO.{stage}: expected exactly one "
                    f"READY marker for {run_id}:{attempt}; found {rows!r}."
                )
            cursor.execute(f'DROP SCHEMA TORONTO."{stage}" CASCADE')
            connection.commit()
            print(
                f"Dropped unused Snowflake RAW candidate TORONTO.{stage}; "
                "production TORONTO.RAW was not changed."
            )
            return True
        finally:
            cursor.close()
            connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    parser.add_argument("--attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
    parser.add_argument("--github-env", type=Path)
    parser.add_argument(
        "--drop-candidate",
        action="store_true",
        help="Drop only this run's untouched READY candidate after a no-op replay.",
    )
    args = parser.parse_args()
    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required.")
    if args.drop_candidate:
        drop_unused_candidate(uri, args.run_id, args.attempt)
    else:
        prepare(uri, args.run_id, args.attempt, args.github_env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
