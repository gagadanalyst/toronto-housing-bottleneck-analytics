"""Atomically promote the validated isolated SCD2 rebuild into TORONTO.RAW."""

from __future__ import annotations

import argparse
import os
import re

from dlt.common.versioned_state import (
    bump_state_version_if_modified,
    compress_state,
    decompress_state,
)

try:
    from snowflake_scd2_historical_replay import SnowflakeSession
except ImportError:
    from scripts.snowflake_scd2_historical_replay import SnowflakeSession

EXPECTED_METRICS = {
    "RAW_ACTIVE_PERMITS": {"total": 104_321, "current": 98_511, "expired": 5_810},
    "RAW_CLEARED_PERMITS": {"total": 190_323, "current": 190_274, "expired": 49},
}
EXPECTED_PIPELINES = {
    "toronto_active_permits_scd2",
    "toronto_cleared_permits_scd2",
}
REQUIRED_TABLES = {
    "RAW_ACTIVE_PERMITS",
    "RAW_CLEARED_PERMITS",
    "TORONTO_SNAPSHOT_LEDGER",
    "_DLT_LOADS",
    "_DLT_VERSION",
    "_DLT_PIPELINE_STATE",
}


def _schema_exists(cursor, schema: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
        "WHERE SCHEMA_NAME = %s",
        (schema,),
    )
    return int(cursor.fetchone()[0]) == 1


def _table_names(cursor, schema: str) -> set[str]:
    cursor.execute(
        "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s",
        (schema,),
    )
    return {str(row[0]).upper() for row in cursor.fetchall()}


def _decode_state(value, pipeline_name: str) -> dict:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = decompress_state(value)
    if not isinstance(value, dict) or not value:
        raise ValueError(f"Invalid DLT state payload for {pipeline_name}.")
    return value


def _assert_schema_baseline(cursor, schema: str, allowed_datasets: set[str]) -> None:
    tables = _table_names(cursor, schema)
    missing = REQUIRED_TABLES - tables
    if missing:
        raise ValueError(f"TORONTO.{schema} is missing tables: {sorted(missing)}")

    for table, expected in EXPECTED_METRICS.items():
        cursor.execute(
            f"SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL), "
            f"COUNT_IF(_DLT_VALID_TO IS NOT NULL) "
            f'FROM TORONTO."{schema}".{table}'
        )
        total, current, expired = (int(value or 0) for value in cursor.fetchone())
        actual = {"total": total, "current": current, "expired": expired}
        if actual != expected:
            raise ValueError(
                f"TORONTO.{schema}.{table} does not match the approved replay: "
                f"expected={expected}, actual={actual}."
            )

    cursor.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT SNAPSHOT_ID), "
        f"COUNT(DISTINCT SOURCE_NAME), "
        f"COUNT_IF(LOWER(STATUS) = 'succeeded'), "
        f"COUNT_IF(ERROR IS NOT NULL) "
        f'FROM TORONTO."{schema}".TORONTO_SNAPSHOT_LEDGER'
    )
    rows, snapshots, sources, succeeded, errors = (
        int(value or 0) for value in cursor.fetchone()
    )
    if (rows, snapshots, sources, succeeded, errors) != (56, 28, 2, 56, 0):
        raise ValueError(
            f"TORONTO.{schema} snapshot ledger is not the exact successful 28-snapshot "
            f"baseline: rows={rows}, snapshots={snapshots}, sources={sources}, "
            f"succeeded={succeeded}, errors={errors}."
        )

    cursor.execute(
        f"SELECT PIPELINE_NAME, STATE, VERSION, ENGINE_VERSION, "
        f"VERSION_HASH, _DLT_LOAD_ID "
        f'FROM TORONTO."{schema}"._DLT_PIPELINE_STATE'
    )
    state_rows = cursor.fetchall()
    state_counts = {name: 0 for name in EXPECTED_PIPELINES}
    for (
        pipeline_value,
        state_value,
        version,
        engine_version,
        version_hash,
        load_id,
    ) in state_rows:
        pipeline = str(pipeline_value).lower()
        if pipeline not in EXPECTED_PIPELINES:
            raise ValueError(f"Unexpected DLT pipeline state row: {pipeline_value!r}.")
        payload = _decode_state(state_value, pipeline)
        if str(payload.get("pipeline_name", "")).lower() != pipeline:
            raise ValueError(f"DLT pipeline state name mismatch for {pipeline}.")
        if str(payload.get("dataset_name", "")).upper() not in allowed_datasets:
            raise ValueError(
                f"DLT state for {pipeline} has unexpected dataset_name "
                f"{payload.get('dataset_name')!r}."
            )
        if str(payload.get("default_schema_name", "")).lower() != pipeline:
            raise ValueError(f"DLT default schema mismatch for {pipeline}.")
        if not isinstance(payload.get("schema_names"), list) or pipeline not in {
            str(item).lower() for item in payload["schema_names"]
        }:
            raise ValueError(f"DLT schema_names is invalid for {pipeline}.")
        if not load_id or not version_hash:
            raise ValueError(
                f"DLT state row is missing its load/hash reference for {pipeline}."
            )
        if (
            int(version) != int(payload.get("_state_version", -1))
            or int(engine_version) != int(payload.get("_state_engine_version", -1))
            or str(version_hash) != str(payload.get("_version_hash", ""))
        ):
            raise ValueError(
                f"DLT state version columns disagree with payload for {pipeline}."
            )
        state_counts[pipeline] += 1

    if any(count == 0 for count in state_counts.values()):
        raise ValueError(f"DLT pipeline state is incomplete: {state_counts}.")

    cursor.execute(
        f"SELECT SCHEMA_NAME, COUNT_IF(STATUS = 0) "
        f'FROM TORONTO."{schema}"._DLT_LOADS GROUP BY SCHEMA_NAME'
    )
    load_counts = {
        str(name).lower(): int(count or 0) for name, count in cursor.fetchall()
    }
    for pipeline in EXPECTED_PIPELINES:
        if load_counts.get(pipeline, 0) < 28:
            raise ValueError(
                f"TORONTO.{schema} has fewer than 28 successful DLT loads "
                f"for {pipeline}."
            )


def _rebase_pipeline_state(cursor, schema: str, source_schema: str) -> int:
    cursor.execute(
        f"SELECT PIPELINE_NAME, STATE, _DLT_LOAD_ID "
        f'FROM TORONTO."{schema}"._DLT_PIPELINE_STATE'
    )
    rows = cursor.fetchall()
    updates = 0
    for pipeline_value, state_value, load_id in rows:
        pipeline = str(pipeline_value).lower()
        if pipeline not in EXPECTED_PIPELINES:
            raise ValueError(f"Unexpected DLT pipeline state row: {pipeline_value!r}.")
        payload = _decode_state(state_value, pipeline)
        dataset_name = str(payload.get("dataset_name", "")).upper()
        if dataset_name == source_schema:
            payload["dataset_name"] = "RAW"
            _version, new_hash, _old_hash = bump_state_version_if_modified(payload)
            new_state = compress_state(payload)
            cursor.execute(
                f'UPDATE TORONTO."{schema}"._DLT_PIPELINE_STATE '
                "SET STATE = %s, VERSION = %s, ENGINE_VERSION = %s, VERSION_HASH = %s "
                "WHERE PIPELINE_NAME = %s AND _DLT_LOAD_ID = %s",
                (
                    new_state,
                    int(payload["_state_version"]),
                    int(payload["_state_engine_version"]),
                    new_hash,
                    pipeline_value,
                    load_id,
                ),
            )
            if int(cursor.rowcount) != 1:
                raise RuntimeError(
                    f"Expected one DLT state update for {pipeline}/{load_id}; "
                    f"updated {cursor.rowcount}."
                )
            updates += 1
        elif dataset_name != "RAW":
            raise ValueError(
                f"DLT state for {pipeline} points to unexpected dataset "
                f"{dataset_name!r}."
            )
    return updates


def promote(source_schema: str) -> None:
    if not re.fullmatch(r"SNOWFLAKE_SCD2_REBUILD_20261107_[0-9]+", source_schema):
        raise ValueError(
            f"Refusing unexpected isolated schema name: {source_schema!r}."
        )

    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required.")

    with SnowflakeSession(uri) as session:
        cursor = session.connection.cursor()
        try:
            if not _schema_exists(cursor, "RAW"):
                raise ValueError("Canonical TORONTO.RAW schema does not exist.")

            # A prior attempt may have completed the schema SWAP but failed while
            # rebasing pipeline state. Detect and finish that state-only operation.
            raw_tables = _table_names(cursor, "RAW")
            if raw_tables >= REQUIRED_TABLES:
                try:
                    _assert_schema_baseline(cursor, "RAW", {"RAW"})
                    print(
                        f"TORONTO.RAW already contains the fully promoted baseline; "
                        f"previous RAW remains retained as TORONTO.{source_schema}."
                    )
                    return
                except ValueError:
                    try:
                        _assert_schema_baseline(cursor, "RAW", {source_schema})
                    except ValueError:
                        pass
                    else:
                        cursor.execute("BEGIN")
                        try:
                            updated = _rebase_pipeline_state(
                                cursor, "RAW", source_schema
                            )
                            session.connection.commit()
                        except Exception:
                            session.connection.rollback()
                            raise
                        _assert_schema_baseline(cursor, "RAW", {"RAW"})
                        print(
                            "PROMOTION RESUME PASS: schema was already swapped; "
                            f"rebased {updated} DLT state rows in TORONTO.RAW."
                        )
                        return

            if not _schema_exists(cursor, source_schema):
                raise ValueError(
                    f"Neither an already-promoted baseline nor the isolated source "
                    f"schema TORONTO.{source_schema} is available."
                )

            _assert_schema_baseline(cursor, source_schema, {source_schema, "RAW"})

            # ALTER SCHEMA SWAP requires ownership of both schemas. Check that the
            # configured ingest role owns them before changing state or grants.
            cursor.execute(
                "SELECT SCHEMA_NAME, SCHEMA_OWNER "
                "FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME IN (%s, %s)",
                ("RAW", source_schema),
            )
            owners = {
                str(name).upper(): str(owner).upper()
                for name, owner in cursor.fetchall()
            }
            if owners.get("RAW") != "TORONTO_INGEST_ROLE":
                raise ValueError(
                    "TORONTO_INGEST_ROLE must own TORONTO.RAW to perform "
                    "the atomic schema swap; "
                    f"observed owner={owners.get('RAW')!r}."
                )
            if owners.get(source_schema) != "TORONTO_INGEST_ROLE":
                raise ValueError(
                    f"TORONTO_INGEST_ROLE must own TORONTO.{source_schema}; "
                    f"observed owner={owners.get(source_schema)!r}."
                )

            # Add dbt read access to the recovered schema. Snowflake swaps schema
            # and object privileges together with the schemas' contents.
            cursor.execute(
                f'GRANT USAGE ON SCHEMA TORONTO."{source_schema}" '
                "TO ROLE TORONTO_DBT_ROLE"
            )
            cursor.execute(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA TORONTO."{source_schema}" '
                "TO ROLE TORONTO_DBT_ROLE"
            )

            # Atomic cutover. The old RAW contents remain available under the
            # isolated schema name, which becomes the rollback copy.
            cursor.execute(
                f'ALTER SCHEMA TORONTO.RAW SWAP WITH TORONTO."{source_schema}"'
            )
            _assert_schema_baseline(cursor, "RAW", {source_schema, "RAW"})

            # DLT loads state from the destination dataset. Rewrite all state rows
            # only after the recovered schema is now physically named RAW. Updating
            # the state version/hash and companion columns preserves DLT integrity.
            cursor.execute("BEGIN")
            try:
                updated = _rebase_pipeline_state(cursor, "RAW", source_schema)
                session.connection.commit()
            except Exception:
                session.connection.rollback()
                raise
            _assert_schema_baseline(cursor, "RAW", {"RAW"})
            print(
                f"PROMOTION PASS: recovered SCD2 history is now TORONTO.RAW; "
                f"previous production RAW is retained as TORONTO.{source_schema}. "
                f"DLT state rows rebased: {updated}."
            )
        finally:
            cursor.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-schema", required=True)
    args = parser.parse_args()
    promote(args.source_schema.upper())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
