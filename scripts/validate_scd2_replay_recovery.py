"""Jointly validate historical DuckDB and Snowflake recovery state."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

import duckdb

try:
    from snowflake_scd2_historical_replay import SnowflakeSession
except ImportError:
    from scripts.snowflake_scd2_historical_replay import SnowflakeSession

try:
    from migrate_legacy_snapshot_artifacts import (
        EXPECTED_LEGACY_RUN_ATTEMPTS,
        EXPECTED_LEGACY_RUN_IDS,
    )
except ImportError:
    from scripts.migrate_legacy_snapshot_artifacts import (
        EXPECTED_LEGACY_RUN_ATTEMPTS,
        EXPECTED_LEGACY_RUN_IDS,
    )

SOURCE_NAMES = ("active_permits", "cleared_permits")
PIPELINE_NAMES = {
    "active_permits": "toronto_active_permits_scd2",
    "cleared_permits": "toronto_cleared_permits_scd2",
}
EXPECTED = {
    (str(run_id), str(EXPECTED_LEGACY_RUN_ATTEMPTS.get(run_id, "1")))
    for run_id in EXPECTED_LEGACY_RUN_IDS
}


def _utc(value) -> str:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Ledger extraction timestamp is timezone-naive.")
    return value.astimezone(UTC).isoformat()


def _expected_summary(path: Path, label: str) -> dict:
    summary = json.loads(path.read_text(encoding="utf-8"))
    if summary.get("status") != "passed" or int(summary.get("snapshot_count", 0)) != 28:
        raise ValueError(f"{label} replay summary is not a passed 28-snapshot result.")
    expected_snapshot_ids = {f"{run_id}:{attempt}" for run_id, attempt in EXPECTED}
    snapshot_ids = summary.get("snapshot_ids", [])
    if (
        not isinstance(snapshot_ids, list)
        or len(snapshot_ids) != 28
        or len(set(snapshot_ids)) != 28
        or set(snapshot_ids) != expected_snapshot_ids
    ):
        raise ValueError(
            f"{label} summary does not identify exactly the approved 28 snapshots."
        )
    for source in SOURCE_NAMES:
        loads = summary.get("sources", {}).get(source, {}).get("snapshot_loads", [])
        identities = {
            (str(row.get("run_id")), str(row.get("run_attempt"))) for row in loads
        }
        if len(loads) != 28 or identities != EXPECTED:
            raise ValueError(
                f"{label} {source} summary does not contain 28 unique source loads."
            )
        for row in loads:
            if (
                not re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256", "")))
                or int(row.get("source_rows", 0)) < 1
            ):
                raise ValueError(f"{label} {source} has invalid source file evidence.")
            if not re.fullmatch(r"[0-9a-f]{40}", str(row.get("source_commit", ""))):
                raise ValueError(f"{label} {source} has an invalid source commit.")
            if not str(row.get("release_tag", "")).startswith(
                "toronto-raw-snapshot-archive-"
            ):
                raise ValueError(f"{label} {source} has no permanent Release identity.")
            expected_asset = (
                f"toronto-raw-snapshot-bundle-{row['run_id']}-{row['run_attempt']}.zip"
            )
            if row.get("asset_name") != expected_asset:
                raise ValueError(
                    f"{label} {source} archive asset name mismatch: {row.get('asset_name')!r}."
                )
            _utc(row.get("extracted_at_utc", ""))
            _utc(row.get("extraction_started_at_utc", ""))
    return summary


def _ledger_records(rows, label: str) -> dict:
    records = {}
    for row in rows:
        (
            snapshot_id,
            run_id,
            attempt,
            started,
            source,
            status,
            commit,
            release,
            asset,
            error,
        ) = row
        key = (str(run_id), str(attempt), str(source))
        if key in records:
            raise ValueError(f"{label}: duplicate ledger entry {key}.")
        if (str(run_id), str(attempt)) not in EXPECTED:
            raise ValueError(f"{label}: unexpected snapshot identity {key}.")
        if str(snapshot_id) != f"{run_id}:{attempt}":
            raise ValueError(f"{label}: inconsistent snapshot_id for {key}.")
        if source not in SOURCE_NAMES:
            raise ValueError(f"{label}: unexpected source name {source!r}.")
        if str(status).lower() != "succeeded" or error:
            raise ValueError(
                f"{label}: source load is not successful: {key}, {status}, {error}."
            )
        if not re.fullmatch(r"[0-9a-f]{40}", str(commit)):
            raise ValueError(f"{label}: invalid source commit for {key}.")
        if not str(release).startswith("toronto-raw-snapshot-archive-"):
            raise ValueError(f"{label}: invalid Release tag for {key}.")
        if str(asset) != f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip":
            raise ValueError(f"{label}: invalid archive asset for {key}.")
        records[key] = {
            "snapshot_id": str(snapshot_id),
            "extraction_started_at_utc": _utc(started),
            "source_commit": str(commit),
            "release_tag": str(release),
            "asset_name": str(asset),
        }
    expected_keys = {
        (run_id, attempt, source)
        for run_id, attempt in EXPECTED
        for source in SOURCE_NAMES
    }
    if set(records) != expected_keys:
        raise ValueError(
            f"{label}: expected 56 unique ledger entries; found {len(records)}; "
            f"missing={sorted(expected_keys - set(records))}, extra={sorted(set(records) - expected_keys)}"
        )
    return records


def _state_payload(state, pipeline_name: str, label: str) -> dict:
    """Decode a persisted DLT 1.31.0 pipeline-state payload."""
    if isinstance(state, memoryview):
        state = state.tobytes()
    if isinstance(state, (bytes, bytearray)):
        try:
            state = bytes(state).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{label}: pipeline state is not UTF-8 for {pipeline_name!r}."
            ) from exc
    if isinstance(state, str):
        try:
            from dlt.common.versioned_state import decompress_state

            state = decompress_state(state)
        except Exception as exc:
            raise ValueError(
                f"{label}: pipeline state is not valid JSON or DLT compressed state for {pipeline_name!r}."
            ) from exc
    if not isinstance(state, dict) or not state:
        raise ValueError(
            f"{label}: empty or non-object DLT pipeline state for {pipeline_name!r}."
        )
    return state


def _metadata_counts(
    load_rows, state_rows, version_rows, label: str, expected_dataset: str
) -> dict:
    """Validate completed DLT loads, schema versions, and actual state checkpoints.

    In this loader, each canonical pipeline creates its default DLT schema from
    its pipeline name. DLT 1.31.0 normalizes that name when constructing the
    schema. The canonical names here are already normalized, so _dlt_loads and
    _dlt_version can be attributed to their owning pipeline by schema_name.
    Pipeline state rows are sparse checkpoints and are not used as a load count.
    """
    expected_pipelines = {item.lower() for item in PIPELINE_NAMES.values()}
    if len(expected_pipelines) != 2:
        raise ValueError(
            f"{label}: expected exactly two canonical DLT pipelines; found {len(expected_pipelines)}."
        )

    loads = {}
    completed_by_pipeline = {pipeline: set() for pipeline in expected_pipelines}
    for load_id, schema_name, status, schema_hash in load_rows:
        key = str(load_id)
        if not key or key.lower() in {"none", "null"}:
            raise ValueError(f"{label}: DLT load has an empty load ID.")
        if key in loads:
            raise ValueError(f"{label}: duplicate dlt load IDs.")
        schema = str(schema_name).lower()
        if not schema or schema in {"none", "null"}:
            raise ValueError(f"{label}: DLT load {key!r} has an empty schema name.")
        if schema not in expected_pipelines:
            raise ValueError(
                f"{label}: unexpected DLT load schema {schema_name!r} for load {key!r}."
            )
        try:
            normalized_status = int(status)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label}: invalid DLT load status for {key!r}.") from exc
        loads[key] = (schema, normalized_status, schema_hash)
        if normalized_status == 0:
            completed_by_pipeline[schema].add(key)

    for pipeline, load_ids in completed_by_pipeline.items():
        if len(load_ids) < 28:
            raise ValueError(
                f"{label}: {pipeline} has fewer than 28 completed DLT loads; found {len(load_ids)}."
            )

    if not version_rows:
        raise ValueError(f"{label}: _dlt_version is empty.")
    version_keys = {
        (str(schema_name).lower(), str(version_hash))
        for schema_name, version_hash in version_rows
        if schema_name is not None and version_hash is not None
    }
    for load_id, (schema_name, status, schema_hash) in loads.items():
        if status == 0 and (
            schema_hash is None or (schema_name, str(schema_hash)) not in version_keys
        ):
            raise ValueError(
                f"{label}: completed DLT load {load_id} references a missing schema version for {schema_name}."
            )

    state_load_ids = {pipeline: set() for pipeline in expected_pipelines}
    state_counts = {pipeline: 0 for pipeline in expected_pipelines}
    for pipeline_name, state, load_id in state_rows:
        name = str(pipeline_name).lower()
        load_id = str(load_id)
        if name not in expected_pipelines:
            raise ValueError(
                f"{label}: unexpected DLT pipeline state {pipeline_name!r}."
            )
        payload = _state_payload(state, str(pipeline_name), label)
        if str(payload.get("pipeline_name", "")).lower() != name:
            raise ValueError(
                f"{label}: DLT state payload pipeline_name does not match its metadata row ({pipeline_name!r})."
            )
        if str(payload.get("dataset_name", "")).lower() != expected_dataset.lower():
            raise ValueError(
                f"{label}: DLT state payload dataset_name does not match target {expected_dataset!r} for {pipeline_name!r}."
            )
        expected_schema = name
        if str(payload.get("default_schema_name", "")).lower() != expected_schema:
            raise ValueError(
                f"{label}: DLT state default_schema_name does not match the canonical pipeline schema for {pipeline_name!r}."
            )
        schema_names = payload.get("schema_names")
        if not isinstance(schema_names, list) or expected_schema not in {
            str(item).lower() for item in schema_names
        }:
            raise ValueError(
                f"{label}: DLT state schema_names does not include the canonical pipeline schema for {pipeline_name!r}."
            )
        if not load_id or load_id.lower() in {"none", "null"}:
            raise ValueError(
                f"{label}: DLT pipeline state has an empty load reference."
            )
        if load_id not in completed_by_pipeline[name]:
            owner = next(
                (
                    pipeline
                    for pipeline, load_ids in completed_by_pipeline.items()
                    if load_id in load_ids
                ),
                None,
            )
            if owner is not None:
                raise ValueError(
                    f"{label}: pipeline state {pipeline_name!r} references load {load_id!r} belonging to {owner!r}."
                )
            raise ValueError(
                f"{label}: pipeline state is not linked to a completed load ({pipeline_name!r}, {load_id})."
            )
        if load_id in state_load_ids[name]:
            raise ValueError(
                f"{label}: duplicate pipeline-state checkpoint for {pipeline_name!r}/{load_id}."
            )
        state_load_ids[name].add(load_id)
        state_counts[name] += 1

    for pipeline in expected_pipelines:
        if state_counts[pipeline] == 0:
            raise ValueError(f"{label}: {pipeline} has no pipeline-state checkpoint.")

    pipeline_names = sorted(expected_pipelines)
    if state_load_ids[pipeline_names[0]] & state_load_ids[pipeline_names[1]]:
        raise ValueError(
            f"{label}: Active and Cleared pipeline states share DLT load IDs."
        )

    return {
        "completed_loads": sum(
            len(load_ids) for load_ids in completed_by_pipeline.values()
        ),
        "completed_loads_per_pipeline": {
            pipeline: len(load_ids)
            for pipeline, load_ids in completed_by_pipeline.items()
        },
        "load_records": len(load_rows),
        "schema_versions": len(version_rows),
        "pipeline_state_rows": state_counts,
        "distinct_state_load_ids_per_pipeline": {
            pipeline: len(load_ids) for pipeline, load_ids in state_load_ids.items()
        },
    }


def _duckdb_checks(database: Path, dataset: str):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dataset):
        raise ValueError(f"Unsafe DuckDB dataset identifier: {dataset!r}")
    with duckdb.connect(str(database), read_only=True) as db:
        tables = {
            str(row[0]).lower()
            for row in db.execute(
                "SELECT table_name FROM information_schema.tables WHERE lower(table_schema)=lower(?)",
                [dataset],
            ).fetchall()
        }
        required = {"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"}
        if not required.issubset(tables):
            raise ValueError(
                f"DuckDB is missing DLT metadata tables: {sorted(required - tables)}"
            )
        ledger = db.execute(
            "SELECT snapshot_id, source_run_id, source_run_attempt, extraction_started_at_utc, "
            "source_name, status, source_commit, release_tag, asset_name, error "
            "FROM raw.toronto_snapshot_ledger"
        ).fetchall()
        loads = db.execute(
            f'SELECT load_id, schema_name, status, schema_version_hash FROM "{dataset}"."_dlt_loads"'
        ).fetchall()
        states = db.execute(
            f'SELECT pipeline_name, state, _dlt_load_id FROM "{dataset}"."_dlt_pipeline_state"'
        ).fetchall()
        versions = db.execute(
            f'SELECT schema_name, version_hash FROM "{dataset}"."_dlt_version"'
        ).fetchall()
    return _ledger_records(ledger, "DuckDB"), _metadata_counts(
        loads, states, versions, "DuckDB", dataset
    )


def _snowflake_checks(session, schema: str):
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", schema):
        raise ValueError(f"Unsafe Snowflake schema: {schema!r}")
    cursor = session.connection.cursor()
    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s",
            (schema,),
        )
        tables = {str(row[0]).lower() for row in cursor.fetchall()}
        required = {
            "_dlt_loads",
            "_dlt_version",
            "_dlt_pipeline_state",
            "toronto_snapshot_ledger",
        }
        if not required.issubset(tables):
            raise ValueError(
                f"Snowflake schema is missing required metadata/ledger tables: {sorted(required - tables)}"
            )
        prefix = f'TORONTO."{schema}"'
        cursor.execute(
            f"SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT, EXTRACTION_STARTED_AT_UTC, "
            f"SOURCE_NAME, STATUS, SOURCE_COMMIT, RELEASE_TAG, ASSET_NAME, ERROR "
            f"FROM {prefix}.TORONTO_SNAPSHOT_LEDGER"
        )
        ledger = cursor.fetchall()
        cursor.execute(
            f"SELECT LOAD_ID, SCHEMA_NAME, STATUS, SCHEMA_VERSION_HASH FROM {prefix}._DLT_LOADS"
        )
        loads = cursor.fetchall()
        cursor.execute(
            f"SELECT PIPELINE_NAME, STATE, _DLT_LOAD_ID FROM {prefix}._DLT_PIPELINE_STATE"
        )
        states = cursor.fetchall()
        cursor.execute(f"SELECT SCHEMA_NAME, VERSION_HASH FROM {prefix}._DLT_VERSION")
        versions = cursor.fetchall()
    finally:
        cursor.close()
    return _ledger_records(ledger, "Snowflake"), _metadata_counts(
        loads, states, versions, "Snowflake", schema
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--dataset", default="raw")
    parser.add_argument("--snowflake-schema", required=True)
    parser.add_argument("--duckdb-summary", type=Path, required=True)
    parser.add_argument("--snowflake-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    duck_summary = _expected_summary(args.duckdb_summary, "DuckDB")
    snow_summary = _expected_summary(args.snowflake_summary, "Snowflake")
    if duck_summary["snapshot_ids"] != snow_summary["snapshot_ids"]:
        raise ValueError(
            "DuckDB and Snowflake replay summaries identify different archive sets."
        )
    duck_ledger, duck_metadata = _duckdb_checks(args.database, args.dataset)
    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError(
            "SNOWFLAKE_DLT_CONNECTION is required for joint recovery validation."
        )
    with SnowflakeSession(uri) as session:
        snow_ledger, snow_metadata = _snowflake_checks(
            session, args.snowflake_schema.upper()
        )

    if duck_ledger != snow_ledger:
        changed = [
            key
            for key in set(duck_ledger) & set(snow_ledger)
            if duck_ledger[key] != snow_ledger[key]
        ]
        raise ValueError(
            "DuckDB and Snowflake application ledgers differ; "
            f"missing_in_snowflake={sorted(set(duck_ledger) - set(snow_ledger))}, "
            f"missing_in_duckdb={sorted(set(snow_ledger) - set(duck_ledger))}, changed={changed}"
        )

    # Each ledger's source identities and immutable archive identity must also
    # agree with the 28 source records in its own replay evidence.
    for label, summary in (("DuckDB", duck_summary), ("Snowflake", snow_summary)):
        for source in SOURCE_NAMES:
            for record in summary["sources"][source]["snapshot_loads"]:
                key = (str(record["run_id"]), str(record["run_attempt"]), source)
                source_ledger = duck_ledger if label == "DuckDB" else snow_ledger
                ledger_row = source_ledger[key]
                expected_started = (
                    datetime.fromisoformat(
                        str(record["extraction_started_at_utc"]).replace("Z", "+00:00")
                    )
                    .astimezone(UTC)
                    .isoformat()
                )
                if (
                    ledger_row["extraction_started_at_utc"] != expected_started
                    or ledger_row["source_commit"] != record["source_commit"]
                    or ledger_row["release_tag"] != record["release_tag"]
                    or ledger_row["asset_name"] != record["asset_name"]
                ):
                    raise ValueError(
                        f"{label}: ledger/archive identity mismatch for {key}."
                    )

    result = {
        "status": "passed",
        "snapshot_count": 28,
        "ledger_entries_per_warehouse": 56,
        "ledger_parity": "passed",
        "duckdb_dlt": duck_metadata,
        "snowflake_dlt": snow_metadata,
        "snowflake_schema": args.snowflake_schema.upper(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        "PASS: both 56-entry ledgers match; both DLT metadata sets are complete and valid."
    )


if __name__ == "__main__":
    main()
