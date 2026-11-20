"""Prepare, validate and promote independent Mode 1 production candidates.

DuckDB state is stored in immutable GitHub Release ZIP assets. Snowflake's
working schema remains in Snowflake; its Release asset stores the matching
SQLite replay ledger and a pointer to that schema. Each incremental replay
starts from the current published DuckDB warehouse or canonical Snowflake RAW,
uses an isolated candidate, and promotes only after validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.replay_ledger import ReplayLedger

STATE_RELEASE_TAG = "toronto-mode1-working-state"
STATE_FORMAT_VERSION = 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _backend(value: str) -> str:
    backend = str(value).lower()
    if backend not in {"duckdb", "snowflake"}:
        raise ValueError(f"Unsupported Mode 1 backend: {value!r}.")
    return backend


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True)
    if check and result.returncode:
        message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n{message}"
        )
    return result


def _generation_parts(generation_id: str) -> tuple[str, str]:
    parts = str(generation_id).split(":")
    if len(parts) != 2 or not all(part.isdecimal() for part in parts):
        raise ValueError(
            "Mode 2 generation ID must use numeric run_id:run_attempt form."
        )
    return parts[0], parts[1]


def _mode2_generation(
    ledger_path: Path, backend: str, generation_id: str
) -> dict[str, Any]:
    """Read and validate a terminal full-history generation and its source ledger."""
    backend = _backend(backend)
    _generation_parts(generation_id)
    ledger_path = Path(ledger_path).resolve()
    if not ledger_path.is_file():
        raise FileNotFoundError(f"Mode 2 ledger is missing: {ledger_path}")

    with closing(
        sqlite3.connect(f"{ledger_path.as_uri()}?mode=ro", uri=True)
    ) as connection:
        connection.row_factory = sqlite3.Row
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not {"replay_loads", "replay_generations"}.issubset(tables):
            raise RuntimeError("Mode 2 ledger is missing generation/source tables.")

        generation = connection.execute(
            """SELECT backend, generation_id, mode, history_scope, candidate,
                      selected_snapshot_count, status
               FROM replay_generations
               WHERE backend = ? AND generation_id = ?""",
            (backend, generation_id),
        ).fetchone()
        if generation is None:
            raise RuntimeError(
                f"No Mode 2 generation exists for {backend}/{generation_id}."
            )
        if generation["status"] != "SUCCEEDED":
            raise RuntimeError(
                f"Mode 2 generation {backend}/{generation_id} is not SUCCEEDED."
            )
        if generation["mode"] != "rebuild" or generation["history_scope"] != "full":
            raise RuntimeError(
                "Mode 1 requires a SUCCEEDED full-history Mode 2 rebuild."
            )

        selected_count = int(generation["selected_snapshot_count"])
        candidate = str(generation["candidate"]).strip()
        if selected_count < 1 or not candidate:
            raise RuntimeError("Mode 2 generation candidate/count metadata is invalid.")

        rows = connection.execute(
            """SELECT source_name, snapshot_id, order_timestamp, run_id,
                      run_attempt, status, format_version, source_commit, archive_sha256,
                      release_tag, asset_name
               FROM replay_loads WHERE backend = ?
               ORDER BY order_timestamp, run_id, run_attempt, snapshot_id,
                        source_name""",
            (backend,),
        ).fetchall()

    snapshots: dict[
        str, tuple[float, int, int, str, tuple[int, str, str, str, str], set[str]]
    ] = {}
    for row in rows:
        snapshot_id = str(row["snapshot_id"])
        source_name = str(row["source_name"])
        if source_name not in {"active_permits", "cleared_permits"}:
            raise RuntimeError(f"Unsupported Mode 2 source name: {source_name}.")
        if str(row["status"]) != "succeeded":
            raise RuntimeError(
                f"Mode 2 source is not succeeded: {snapshot_id}/{source_name}."
            )
        order = (
            float(row["order_timestamp"]),
            int(row["run_id"]),
            int(row["run_attempt"]),
            snapshot_id,
        )
        source_identity = (
            int(row["format_version"]),
            str(row["source_commit"]),
            str(row["archive_sha256"]),
            str(row["release_tag"]),
            str(row["asset_name"]),
        )
        current = snapshots.get(snapshot_id)
        if current is None:
            snapshots[snapshot_id] = (*order, source_identity, {source_name})
        else:
            # Active and Cleared are extracted independently, so their
            # observation timestamps may legitimately differ within one
            # immutable archive. The archive identity must match; retain the
            # later source timestamp for chronological baseline selection.
            if (
                current[1:4] != order[1:]
                or current[4] != source_identity
                or source_name in current[5]
            ):
                raise RuntimeError(
                    f"Mode 2 source identity is duplicated or inconsistent: {snapshot_id}."
                )
            current[5].add(source_name)
            if order[0] > current[0]:
                snapshots[snapshot_id] = (order[0], *current[1:])

    if len(snapshots) != selected_count:
        raise RuntimeError(
            f"Mode 2 selected {selected_count} snapshots but its ledger contains "
            f"{len(snapshots)}."
        )
    for snapshot_id, item in snapshots.items():
        if item[5] != {"active_permits", "cleared_permits"}:
            raise RuntimeError(
                f"Mode 2 snapshot {snapshot_id} lacks one of its two source records."
            )

    latest = max(snapshots.values(), key=lambda item: item[:4])
    return {
        "backend": backend,
        "generation_id": generation_id,
        "candidate": candidate,
        "selected_snapshot_count": selected_count,
        "baseline_snapshot_id": latest[3],
        "baseline_snapshot_count": selected_count,
    }


def _copy_atomic(source: Path, destination: Path) -> None:
    source = Path(source)
    destination = Path(destination)
    if not source.is_file() or source.stat().st_size <= 0:
        raise FileNotFoundError(f"Candidate file is missing or empty: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        shutil.copy2(source, temporary)
        if _sha256(source) != _sha256(temporary):
            raise RuntimeError("Candidate copy failed SHA-256 verification.")
        if destination.exists():
            raise FileExistsError(
                f"Refusing to overwrite working candidate: {destination}"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def bootstrap_duckdb_working_state(
    *,
    mode2_ledger_path: Path,
    mode2_candidate_path: Path,
    working_dir: Path,
    generation_id: str,
) -> dict[str, Any]:
    """One-time copy of accepted Mode 2 DuckDB and SQLite state into Mode 1."""
    metadata = _mode2_generation(mode2_ledger_path, "duckdb", generation_id)
    candidate_source = Path(mode2_candidate_path).resolve()
    mode2_run_id, _mode2_attempt = _generation_parts(generation_id)
    expected_candidate_name = f"toronto-replay-{mode2_run_id}.duckdb"
    if candidate_source.name != expected_candidate_name:
        raise RuntimeError(
            "Supplied DuckDB file is not the candidate named by this Mode 2 run."
        )
    if not candidate_source.is_file():
        raise FileNotFoundError(
            f"Accepted Mode 2 DuckDB candidate is missing: {candidate_source}"
        )

    working_dir = Path(working_dir).resolve()
    if working_dir.exists() and any(working_dir.iterdir()):
        raise FileExistsError(f"Mode 1 working directory is not empty: {working_dir}")
    working_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{working_dir.name}.", dir=working_dir.parent)
    )
    try:
        working_ledger = temporary_dir / "replay-ledger.sqlite"
        ReplayLedger.bootstrap_working_ledger(
            mode2_ledger_path,
            working_ledger,
            "duckdb",
            generation_id,
        )
        candidate = temporary_dir / "candidate.duckdb"
        if Path(str(metadata["candidate"])).name != candidate_source.name:
            raise RuntimeError(
                "Mode 2 generation candidate name does not match the supplied candidate file."
            )
        _copy_atomic(candidate_source, candidate)
        metadata["mode2_candidate"] = metadata.pop("candidate")
        metadata.update(
            {
                "format_version": STATE_FORMAT_VERSION,
                "candidate": candidate.name,
                "candidate_sha256": _sha256(candidate),
                "candidate_file": candidate.name,
                "created_at_utc": datetime.now(UTC).isoformat(),
            }
        )
        (temporary_dir / "manifest.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        working_dir.rmdir() if working_dir.exists() else None
        os.replace(temporary_dir, working_dir)
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise
    return metadata


def _snowflake_connection(credentials: str):
    """Return a Snowflake connection with the project role/account checks applied."""
    from scripts.replay_targets import (
        SnowflakeReplayTarget,
        _validate_snowflake_account_context,
    )
    from scripts.snowflake_operations import require_snowflake_operations_enabled

    require_snowflake_operations_enabled("Mode 1 Snowflake state preparation")
    import snowflake.connector

    values, key_bytes, passphrase = SnowflakeReplayTarget._parse_credentials(
        credentials
    )
    temporary = tempfile.TemporaryDirectory(prefix="toronto-mode1-snowflake-")
    key_path = Path(temporary.name) / "private_key.p8"
    key_path.write_bytes(key_bytes)
    key_path.chmod(0o600)
    options = {**values, "private_key_file": str(key_path)}
    if passphrase:
        options["private_key_file_pwd"] = passphrase
    connection = None
    try:
        connection = snowflake.connector.connect(**options)
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), "
                "CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
            )
            _validate_snowflake_account_context(*cursor.fetchone())
        finally:
            cursor.close()
    except Exception:
        if connection is not None:
            connection.close()
        temporary.cleanup()
        raise
    return connection, temporary


def _manifest_snowflake_generation(manifest: dict[str, Any]) -> str:
    """Return the generation identity of the persisted Snowflake candidate."""
    generation_id = manifest.get("candidate_generation_id")
    if generation_id is None:
        run_id = manifest.get("last_attempt_run_id")
        run_attempt = manifest.get("last_attempt_run_attempt")
        if run_id is None or run_attempt is None:
            raise RuntimeError(
                "Snowflake Mode 1 checkpoint is missing its candidate generation identity."
            )
        generation_id = f"{run_id}:{run_attempt}"
    _generation_parts(str(generation_id))
    return str(generation_id)


def _snowflake_raw_mode1_marker_state(
    cursor: Any, *, expected_generation_id: str
) -> str:
    """Read the exact generation marker after or during a schema swap."""
    _generation_parts(expected_generation_id)
    cursor.execute(
        "SELECT GENERATION_ID, PROMOTION_STATE "
        "FROM TORONTO.RAW.TORONTO_CANDIDATE_CONTROL"
    )
    rows = cursor.fetchall()
    if len(rows) != 1 or str(rows[0][0]) != expected_generation_id:
        raise RuntimeError(
            "Canonical TORONTO.RAW does not carry the expected Mode 1 generation "
            f"{expected_generation_id}: {rows!r}."
        )
    state = str(rows[0][1]).upper()
    if state not in {"READY", "PROMOTED"}:
        raise RuntimeError(
            f"Canonical TORONTO.RAW has an unsupported Mode 1 promotion state: {rows!r}."
        )
    return state


def _validate_snowflake_promoted_mode1_raw_marker(
    cursor: Any, *, expected_generation_id: str
) -> None:
    """Require canonical RAW to carry the exact promoted Mode 1 generation."""
    state = _snowflake_raw_mode1_marker_state(
        cursor, expected_generation_id=expected_generation_id
    )
    if state != "PROMOTED":
        raise RuntimeError(
            "Canonical TORONTO.RAW does not carry the expected promoted "
            f"Mode 1 marker for {expected_generation_id}: state={state}."
        )


def _canonical_snowflake_mode1_marker_state(
    credentials: str, expected_generation_id: str
) -> str:
    """Read canonical RAW's exact Mode 1 marker under the required owner."""
    connection, temporary = _snowflake_connection(credentials)
    try:
        cursor = connection.cursor()
        try:
            from scripts.replay_targets import _validate_snowflake_schema_owner

            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME='RAW'"
            )
            _validate_snowflake_schema_owner("RAW", cursor.fetchone())
            return _snowflake_raw_mode1_marker_state(
                cursor, expected_generation_id=expected_generation_id
            )
        finally:
            cursor.close()
    finally:
        connection.close()
        temporary.cleanup()


def _replace_candidate_from_production(production: Path, candidate: Path) -> None:
    """Atomically refresh a DuckDB working candidate from published production."""
    production = Path(production).resolve()
    candidate = Path(candidate).resolve()
    if production == candidate:
        raise ValueError("Production and Mode 1 DuckDB candidate paths must differ.")
    if not production.is_file() or production.stat().st_size <= 0:
        raise FileNotFoundError(
            f"Published DuckDB production database is missing or empty: {production}"
        )
    candidate.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=candidate.name + ".prepare-",
        suffix=".tmp",
        dir=candidate.parent,
        delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        shutil.copy2(production, staged)
        if staged.stat().st_size != production.stat().st_size:
            raise RuntimeError("Production-to-candidate DuckDB copy size mismatch.")
        if _sha256(staged) != _sha256(production):
            raise RuntimeError("Production-to-candidate DuckDB copy checksum mismatch.")
        os.replace(staged, candidate)
        from scripts.prepare_duckdb_candidate import _ensure_quarantine_table

        _ensure_quarantine_table(candidate)
    finally:
        staged.unlink(missing_ok=True)


def _candidate_quarantine_baseline(
    *, backend: str, working_dir: Path, schema: str | None, credentials: str | None
) -> tuple[dict[str, int], list[str]]:
    """Read promotion quarantine denominators and completed paired snapshot IDs."""
    source_tables = {
        "active_permits": (
            "RAW_ACTIVE_PERMITS" if backend == "snowflake" else "raw_active_permits"
        ),
        "cleared_permits": (
            "RAW_CLEARED_PERMITS" if backend == "snowflake" else "raw_cleared_permits"
        ),
    }
    counts: dict[str, int] = {}
    ledger_rows: list[tuple[Any, Any, Any]]

    if backend == "duckdb":
        import duckdb

        database = Path(working_dir) / "candidate.duckdb"
        if not database.is_file():
            raise FileNotFoundError(f"Mode 1 DuckDB candidate is missing: {database}")
        with duckdb.connect(str(database), read_only=True) as connection:
            names = {
                str(row[0]).lower()
                for row in connection.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='raw'"
                ).fetchall()
            }
            required = set(source_tables.values()) | {"toronto_snapshot_ledger"}
            if not required.issubset(names):
                raise RuntimeError(
                    "DuckDB production-derived candidate is missing RAW tables: "
                    f"{sorted(required - names)}."
                )
            for source_name, table in source_tables.items():
                counts[source_name] = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM raw.{table} WHERE _dlt_valid_to IS NULL"
                    ).fetchone()[0]
                )
            ledger_rows = connection.execute(
                "SELECT snapshot_id, source_name, status "
                "FROM raw.toronto_snapshot_ledger"
            ).fetchall()
    elif backend == "snowflake":
        from scripts.replay_targets import (
            INCREMENTAL_SNOWFLAKE_SCHEMA_RE,
            _validate_snowflake_schema_owner,
        )

        schema = str(schema or "").upper()
        if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema):
            raise ValueError(
                f"Unapproved Mode 1 Snowflake candidate schema: {schema!r}."
            )
        if not credentials:
            raise ValueError(
                "SNOWFLAKE_DLT_CONNECTION is required for Mode 1 promotion baselines."
            )
        connection, temporary = _snowflake_connection(credentials)
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME=%s",
                    (schema,),
                )
                _validate_snowflake_schema_owner(schema, cursor.fetchone())
                for source_name, table in source_tables.items():
                    cursor.execute(
                        f"SELECT COUNT_IF(_DLT_VALID_TO IS NULL) "
                        f'FROM TORONTO."{schema}".{table}'
                    )
                    counts[source_name] = int(cursor.fetchone()[0] or 0)
                cursor.execute(
                    f"SELECT SNAPSHOT_ID, SOURCE_NAME, STATUS "
                    f'FROM TORONTO."{schema}".TORONTO_SNAPSHOT_LEDGER'
                )
                ledger_rows = cursor.fetchall()
            finally:
                cursor.close()
        finally:
            connection.close()
            temporary.cleanup()
    else:
        raise ValueError(f"Unsupported Mode 1 baseline backend: {backend!r}.")

    if set(counts) != set(source_tables) or any(
        value <= 0 for value in counts.values()
    ):
        raise RuntimeError(f"Invalid quarantine current-row baseline counts: {counts}.")

    by_snapshot: dict[str, dict[str, str]] = {}
    for snapshot_id, source_name, status in ledger_rows:
        by_snapshot.setdefault(str(snapshot_id), {})[str(source_name).lower()] = str(
            status
        ).lower()
    expected_sources = {"active_permits", "cleared_permits"}
    snapshot_ids = sorted(
        snapshot_id
        for snapshot_id, statuses in by_snapshot.items()
        if set(statuses) == expected_sources
        and all(statuses[source] == "succeeded" for source in expected_sources)
    )
    if not snapshot_ids:
        raise RuntimeError(
            "Candidate has no completed paired snapshots for quarantine reconciliation."
        )
    return counts, snapshot_ids


def _validated_checkpoint_is_already_promoted(
    *,
    manifest: dict[str, Any],
    observed_baseline: dict[str, Any],
    backend: str,
    production_database: Path | None,
) -> bool:
    """Recognize only the recorded baseline or the exact promoted DuckDB file."""
    expected_baseline = manifest.get("production_baseline")
    if not isinstance(expected_baseline, dict):
        raise RuntimeError(
            "Validated Mode 1 checkpoint is missing its original production baseline."
        )
    if observed_baseline == expected_baseline:
        return False
    if backend == "snowflake":
        # Snowflake promotion is identified by the candidate generation marker in
        # canonical RAW; its aggregate baseline necessarily changes after SWAP.
        return False
    if backend == "duckdb" and production_database is not None:
        candidate_sha256 = manifest.get("candidate_sha256")
        production = Path(production_database)
        if (
            isinstance(candidate_sha256, str)
            and production.is_file()
            and _sha256(production) == candidate_sha256
        ):
            return True
    raise RuntimeError(
        "Production matches neither the original baseline nor the validated "
        "Mode 1 candidate. Refusing promotion recovery."
    )


def _validate_snowflake_candidate_marker(
    cursor: Any, *, schema: str, expected_generation_id: str
) -> None:
    "Require one READY marker matching the checkpoint current run."
    from scripts.replay_targets import INCREMENTAL_SNOWFLAKE_SCHEMA_RE

    schema = str(schema).upper()
    if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema):
        raise ValueError(f"Unapproved Snowflake candidate marker schema: {schema}.")
    _generation_parts(expected_generation_id)
    cursor.execute(
        f'''SELECT GENERATION_ID, PROMOTION_STATE
            FROM TORONTO."{schema}".TORONTO_CANDIDATE_CONTROL'''
    )
    rows = cursor.fetchall()
    if rows != [(expected_generation_id, "READY")]:
        raise RuntimeError(
            "Snowflake Mode 1 checkpoint control marker does not match its "
            f"recorded run {expected_generation_id}: {rows!r}."
        )


def _validate_snowflake_promoted_raw_marker(
    cursor: Any, *, expected_mode2_run_id: str
) -> None:
    """Require canonical RAW to contain the expected promoted Mode 2 marker."""
    expected_mode2_run_id = str(expected_mode2_run_id)
    if not expected_mode2_run_id.isdecimal():
        raise ValueError("Expected Mode 2 run ID must be numeric.")

    cursor.execute(
        "SELECT GENERATION_ID, PROMOTION_STATE "
        "FROM TORONTO.RAW.TORONTO_CANDIDATE_CONTROL"
    )
    rows = cursor.fetchall()
    expected = [(f"rebuild:{expected_mode2_run_id}", "PROMOTED")]
    if rows != expected:
        raise RuntimeError(
            "Canonical TORONTO.RAW does not carry the expected promoted "
            f"Mode 2 marker for run {expected_mode2_run_id}: {rows!r}."
        )


def _validate_snowflake_checkpoint_schema(
    *, credentials: str, schema: str, expected_generation_id: str
) -> None:
    "Validate schema ownership and marker before accepting a checkpoint."
    from scripts.replay_targets import (
        INCREMENTAL_SNOWFLAKE_SCHEMA_RE,
        _validate_snowflake_schema_owner,
    )

    schema = str(schema).upper()
    if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema):
        raise ValueError(f"Unapproved persisted Mode 1 Snowflake schema: {schema}.")
    connection, temporary = _snowflake_connection(credentials)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME=%s",
                (schema,),
            )
            _validate_snowflake_schema_owner(schema, cursor.fetchone())
            _validate_snowflake_candidate_marker(
                cursor,
                schema=schema,
                expected_generation_id=expected_generation_id,
            )
        finally:
            cursor.close()
    finally:
        connection.close()
        temporary.cleanup()


def _clone_snowflake_schema(
    *,
    credentials: str,
    source_schema: str,
    run_id: str,
    run_attempt: str,
    expected_generation_id: str | None = None,
    expected_mode2_run_id: str | None = None,
    expected_promoted_generation_id: str | None = None,
) -> str:
    "Clone an accepted schema after validating its applicable control marker."
    from scripts.replay_targets import (
        INCREMENTAL_SNOWFLAKE_SCHEMA_RE,
        REBUILD_SNOWFLAKE_SCHEMA_RE,
        _validate_snowflake_schema_owner,
    )
    from scripts.snowflake_candidate_utils import rebase_pipeline_state

    source_schema = str(source_schema).upper()
    if expected_mode2_run_id is not None or expected_promoted_generation_id is not None:
        if (
            source_schema != "RAW"
            or expected_generation_id is not None
            or (
                expected_mode2_run_id is not None
                and expected_promoted_generation_id is not None
            )
        ):
            raise ValueError(
                "A promoted canonical RAW marker is required when bootstrapping "
                "a new Mode 1 Snowflake candidate."
            )
        if (
            expected_mode2_run_id is not None
            and not str(expected_mode2_run_id).isdecimal()
        ):
            raise ValueError("Expected Mode 2 run ID must be numeric.")
        if expected_promoted_generation_id is not None:
            _generation_parts(expected_promoted_generation_id)
    elif not (
        REBUILD_SNOWFLAKE_SCHEMA_RE.fullmatch(source_schema)
        or INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(source_schema)
    ):
        raise ValueError(f"Unapproved Mode 1 source schema: {source_schema}.")
    if not str(run_id).isdecimal() or not str(run_attempt).isdecimal():
        raise ValueError("Workflow run ID and attempt must be numeric.")
    if expected_generation_id is not None:
        if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(source_schema):
            raise ValueError(
                "A persisted Mode 1 marker is required only for a Mode 1 source schema."
            )
        _generation_parts(expected_generation_id)
    target_schema = f"RAW_STAGE_{run_id}_{run_attempt}"
    if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(target_schema):
        raise ValueError("Generated Mode 1 Snowflake candidate schema is invalid.")

    connection, temporary = _snowflake_connection(credentials)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME=%s",
                (source_schema,),
            )
            _validate_snowflake_schema_owner(source_schema, cursor.fetchone())
            if expected_mode2_run_id is not None:
                _validate_snowflake_promoted_raw_marker(
                    cursor,
                    expected_mode2_run_id=expected_mode2_run_id,
                )
            if expected_promoted_generation_id is not None:
                _validate_snowflake_promoted_mode1_raw_marker(
                    cursor,
                    expected_generation_id=expected_promoted_generation_id,
                )
            if expected_generation_id is not None:
                _validate_snowflake_candidate_marker(
                    cursor,
                    schema=source_schema,
                    expected_generation_id=expected_generation_id,
                )
            cursor.execute(
                "SELECT SCHEMA_NAME FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME=%s",
                (target_schema,),
            )
            if cursor.fetchone() is not None:
                raise RuntimeError(
                    f"Mode 1 candidate schema already exists: TORONTO.{target_schema}."
                )
            cursor.execute(
                f'CREATE SCHEMA TORONTO."{target_schema}" CLONE TORONTO."{source_schema}"'
            )
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME=%s",
                (target_schema,),
            )
            _validate_snowflake_schema_owner(target_schema, cursor.fetchone())
            cursor.execute(
                f'''CREATE OR REPLACE TABLE TORONTO."{target_schema}".TORONTO_CANDIDATE_CONTROL (
                    GENERATION_ID VARCHAR NOT NULL,
                    PROMOTION_STATE VARCHAR NOT NULL,
                    UPDATED_AT TIMESTAMP_TZ
                )'''
            )
            cursor.execute(
                f'''INSERT INTO TORONTO."{target_schema}".TORONTO_CANDIDATE_CONTROL
                    (GENERATION_ID, PROMOTION_STATE) VALUES (%s, %s)''',
                (f"{run_id}:{run_attempt}", "READY"),
            )
            rebased_states = rebase_pipeline_state(
                cursor, target_schema, source_schema, target_schema
            )
            connection.commit()
            print(
                f"Rebased {rebased_states} cloned Snowflake DLT state row(s) "
                f"from {source_schema} to {target_schema}."
            )
        finally:
            cursor.close()
    finally:
        connection.close()
        temporary.cleanup()
    return target_schema


def discard_empty_snowflake_candidate(
    *, credentials: str, schema: str, generation_id: str
) -> None:
    """Drop only this run's untouched Mode 1 schema when no archives were pending."""
    from scripts.replay_targets import (
        INCREMENTAL_SNOWFLAKE_SCHEMA_RE,
        _validate_snowflake_schema_owner,
    )
    from scripts.snowflake_operations import require_snowflake_operations_enabled

    require_snowflake_operations_enabled(
        "Discarding an unused Mode 1 Snowflake candidate"
    )
    schema = str(schema).upper()
    if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema):
        raise ValueError(
            "Refusing to drop a schema outside the Mode 1 candidate pattern."
        )
    run_id, run_attempt = _generation_parts(generation_id)
    if schema != f"RAW_STAGE_{run_id}_{run_attempt}":
        raise ValueError(
            "Snowflake schema does not match the supplied run generation ID."
        )

    connection, temporary = _snowflake_connection(credentials)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME=%s",
                (schema,),
            )
            owner = cursor.fetchone()
            if owner is None:
                return
            _validate_snowflake_schema_owner(schema, owner)
            cursor.execute(
                f"""SELECT GENERATION_ID, PROMOTION_STATE
                    FROM TORONTO.\"{schema}\".TORONTO_CANDIDATE_CONTROL"""
            )
            rows = cursor.fetchall()
            if rows != [(generation_id, "READY")]:
                raise RuntimeError(
                    "Refusing to drop a Snowflake candidate whose control marker "
                    f"does not match this unused run: {rows!r}."
                )
            cursor.execute(f'DROP SCHEMA TORONTO."{schema}" CASCADE')
            connection.commit()
        finally:
            cursor.close()
    finally:
        connection.close()
        temporary.cleanup()


def bootstrap_snowflake_working_state(
    *,
    mode2_ledger_path: Path,
    working_dir: Path,
    generation_id: str,
    credentials: str,
    run_id: str,
    run_attempt: str,
) -> dict[str, Any]:
    """Bootstrap the SQLite ledger and clone the accepted Mode 2 Snowflake schema."""
    metadata = _mode2_generation(mode2_ledger_path, "snowflake", generation_id)
    mode2_candidate = str(metadata["candidate"])
    if not mode2_candidate.startswith("TORONTO."):
        raise RuntimeError(f"Invalid accepted Snowflake candidate: {mode2_candidate}.")
    mode2_candidate_schema = mode2_candidate.split(".", 1)[1].strip('"').upper()
    mode2_run_id, _mode2_attempt = _generation_parts(generation_id)
    if mode2_candidate_schema != f"TORONTO_SNAPSHOT_REPLAY_{mode2_run_id}":
        raise RuntimeError(
            "Snowflake schema does not match the accepted Mode 2 generation run ID."
        )

    # Mode 2 promotion swaps the accepted candidate into canonical RAW.
    # The ledger candidate name now refers to the previous production schema.
    source_schema = "RAW"

    working_dir = Path(working_dir).resolve()
    if working_dir.exists() and any(working_dir.iterdir()):
        raise FileExistsError(f"Mode 1 working directory is not empty: {working_dir}")
    working_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{working_dir.name}.", dir=working_dir.parent)
    )
    try:
        ReplayLedger.bootstrap_working_ledger(
            mode2_ledger_path,
            temporary_dir / "replay-ledger.sqlite",
            "snowflake",
            generation_id,
        )
        schema = _clone_snowflake_schema(
            credentials=credentials,
            source_schema=source_schema,
            run_id=run_id,
            run_attempt=run_attempt,
            expected_mode2_run_id=mode2_run_id,
        )
        metadata["mode2_candidate"] = metadata.pop("candidate")
        metadata.update(
            {
                "format_version": STATE_FORMAT_VERSION,
                "candidate_schema": schema,
                "candidate": f"TORONTO.{schema}",
                "mode2_promoted_source_schema": "TORONTO.RAW",
                "created_at_utc": datetime.now(UTC).isoformat(),
            }
        )
        (temporary_dir / "manifest.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        working_dir.rmdir() if working_dir.exists() else None
        os.replace(temporary_dir, working_dir)
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise
    return metadata


def _safe_extract_tar(archive_path: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        seen: set[str] = set()
        for member in members:
            relative = PurePosixPath(member.name)
            normalized = relative.as_posix()
            if normalized in seen:
                raise RuntimeError("Mode 2 state archive contains duplicate paths.")
            seen.add(normalized)
            if not normalized or normalized == ".":
                raise RuntimeError("Mode 2 state archive contains an empty path.")
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError("Mode 2 state archive contains an unsafe path.")
            if not (member.isdir() or member.isfile()):
                raise RuntimeError("Mode 2 state archive contains a non-regular file.")
            target = (destination / Path(*relative.parts)).resolve()
            if not target.is_relative_to(root):
                raise RuntimeError(
                    "Mode 2 state archive escapes its extraction folder."
                )
        for member in members:
            if member.isdir():
                (destination / Path(*PurePosixPath(member.name).parts)).mkdir(
                    parents=True, exist_ok=True
                )
            else:
                target = destination / Path(*PurePosixPath(member.name).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError("Unable to read a Mode 2 state archive member.")
                with source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)


def _download_mode2_artifact(
    *, repository: str, backend: str, run_id: str, run_attempt: str, destination: Path
) -> Path:
    if not str(run_id).isdecimal() or not str(run_attempt).isdecimal():
        raise ValueError("Initial Mode 2 run ID and attempt must be numeric.")
    artifact_name = f"snapshot-loader-{backend}-{run_id}-{run_attempt}"
    destination.mkdir(parents=True, exist_ok=True)
    _run(
        [
            "gh",
            "run",
            "download",
            str(run_id),
            "--repo",
            repository,
            "--name",
            artifact_name,
            "--dir",
            str(destination),
        ]
    )
    archives = list(destination.rglob("state.tar.gz"))
    if len(archives) != 1:
        raise RuntimeError(
            f"Expected exactly one state.tar.gz in {artifact_name}; found {len(archives)}."
        )
    return archives[0]


def restore_state_bundle(bundle_path: Path, working_dir: Path) -> dict[str, Any]:
    """Safely restore a previously published Mode 1 bundle into an empty directory."""
    bundle_path = Path(bundle_path)
    working_dir = Path(working_dir).resolve()
    if working_dir.exists() and any(working_dir.iterdir()):
        raise FileExistsError(f"Mode 1 restore directory is not empty: {working_dir}")
    working_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{working_dir.name}.", dir=working_dir.parent)
    )
    try:
        with zipfile.ZipFile(bundle_path) as archive:
            if archive.testzip() is not None:
                raise RuntimeError("Mode 1 state ZIP failed its integrity check.")
            names = set(archive.namelist())
            required = {"manifest.json", "replay-ledger.sqlite", "summary.json"}
            if not required.issubset(names):
                raise RuntimeError("Mode 1 state ZIP is missing required files.")
            manifest = json.loads(archive.read("manifest.json"))
            backend = _backend(str(manifest.get("backend", "")))
            if manifest.get("format_version") != STATE_FORMAT_VERSION:
                raise RuntimeError("Unsupported Mode 1 state bundle format.")
            handoff_status = manifest.get("handoff_status")
            if handoff_status not in {"baseline", "validated", "retryable"}:
                raise RuntimeError(
                    "Mode 1 state bundle has no accepted handoff status."
                )
            summary = json.loads(archive.read("summary.json"))
            if handoff_status == "baseline":
                summary_valid = (
                    summary.get("mode") == "bootstrap"
                    and summary.get("status") == "passed"
                    and summary.get("baseline_generation_id")
                    == manifest.get("generation_id")
                )
            else:
                expected_summary_status = (
                    "passed" if handoff_status == "validated" else "failed"
                )
                summary_valid = (
                    summary.get("mode") == "incremental"
                    and summary.get("status") == expected_summary_status
                )
            if (
                summary.get("backend") != backend
                or not summary_valid
                or (summary.get("history_validation") or {}).get("status") != "passed"
            ):
                raise RuntimeError(
                    "Mode 1 state summary does not match its handoff status."
                )
            if not manifest.get("generation_id") or not manifest.get("mode2_candidate"):
                raise RuntimeError(
                    "Mode 1 state bundle is missing its accepted Mode 2 identity."
                )
            if backend == "duckdb":
                if manifest.get("candidate") != "candidate.duckdb":
                    raise RuntimeError(
                        "DuckDB state manifest names an unexpected candidate file."
                    )
                if "candidate.duckdb" not in names:
                    raise RuntimeError(
                        "DuckDB state bundle is missing candidate.duckdb."
                    )
                archive.extract("candidate.duckdb", temporary_dir)
                candidate = temporary_dir / "candidate.duckdb"
                if _sha256(candidate) != manifest.get("candidate_sha256"):
                    raise RuntimeError("Mode 1 DuckDB candidate checksum mismatch.")
            else:
                from scripts.replay_targets import INCREMENTAL_SNOWFLAKE_SCHEMA_RE

                schema = str(manifest.get("candidate_schema", "")).upper()
                if (
                    not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema)
                    or manifest.get("candidate") != f"TORONTO.{schema}"
                ):
                    raise RuntimeError(
                        "Snowflake state bundle has an invalid candidate schema."
                    )
                if "candidate.duckdb" in names:
                    raise RuntimeError(
                        "Snowflake state bundle must not contain a DuckDB file."
                    )
            for name in ("manifest.json", "replay-ledger.sqlite", "summary.json"):
                archive.extract(name, temporary_dir)

        ledger = ReplayLedger(temporary_dir / "replay-ledger.sqlite")
        marker = ledger.bootstrap_record(backend)
        if (
            marker is None
            or marker["generation_id"] != manifest.get("generation_id")
            or marker["candidate"] != manifest.get("mode2_candidate")
            or int(marker["selected_snapshot_count"])
            != int(manifest.get("selected_snapshot_count", 0))
        ):
            raise RuntimeError(
                "Mode 1 state ledger bootstrap identity does not match its manifest."
            )

        working_dir.rmdir() if working_dir.exists() else None
        os.replace(temporary_dir, working_dir)
        return manifest
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise


def _release_pointer_name(backend: str, run_id: str, run_attempt: str) -> str:
    return f"toronto-mode1-state-{_backend(backend)}-{run_id}-{run_attempt}.json"


def _state_asset_attempt(run_attempt: str, asset_run_attempt: str | None = None) -> str:
    """Give validated and promoted checkpoints distinct immutable asset names."""
    value = str(asset_run_attempt if asset_run_attempt is not None else run_attempt)
    if not value.isdecimal():
        raise ValueError("Mode 1 state asset attempt must contain digits only.")
    return value


def _release_pointer_candidates(
    assets: list[dict[str, Any]], backend: str
) -> list[tuple[int, int, str]]:
    """Return complete pointer/bundle checkpoints ordered by run ID and attempt."""
    prefix = f"toronto-mode1-state-{_backend(backend)}-"
    names = {str(asset.get("name", "")) for asset in assets}
    candidates: list[tuple[int, int, str]] = []
    for name in names:
        if not name.startswith(prefix) or not name.endswith(".json"):
            continue
        identity = name[len(prefix) : -len(".json")].split("-")
        if len(identity) != 2 or not all(part.isdecimal() for part in identity):
            continue
        bundle_name = f"{prefix}{identity[0]}-{identity[1]}.zip"
        if bundle_name not in names:
            continue
        candidates.append((int(identity[0]), int(identity[1]), name))
    return sorted(candidates)


def _latest_release_pointer(assets: list[dict[str, Any]], backend: str) -> str | None:
    """Select the newest complete immutable pointer/bundle checkpoint."""
    candidates = _release_pointer_candidates(assets, backend)
    return candidates[-1][2] if candidates else None


def _release_assets(repository: str) -> list[dict[str, Any]]:
    result = _run(
        [
            "gh",
            "release",
            "view",
            STATE_RELEASE_TAG,
            "--repo",
            repository,
            "--json",
            "assets",
        ],
        check=False,
    )
    if result.returncode:
        message = (result.stderr or result.stdout).lower()
        if "not found" in message or "404" in message:
            return []
        raise RuntimeError(
            f"Unable to inspect Mode 1 state Release assets: "
            f"{(result.stderr or result.stdout).strip()}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GitHub returned invalid Mode 1 Release asset JSON."
        ) from exc
    assets = payload.get("assets") if isinstance(payload, dict) else None
    if not isinstance(assets, list):
        raise RuntimeError("GitHub Release response is missing its assets list.")
    return assets


def _cleanup_old_release_assets(
    *, repository: str, backend: str, keep_bundle: str, keep_pointer: str
) -> None:
    """Retain the newest two complete checkpoints for this backend."""
    prefix = f"toronto-mode1-state-{_backend(backend)}-"
    try:
        assets = _release_assets(repository)
        retained = _release_pointer_candidates(assets, backend)[-2:]
        keep_names = {keep_bundle, keep_pointer}
        for run_id, run_attempt, pointer_name in retained:
            keep_names.add(pointer_name)
            keep_names.add(f"{prefix}{run_id}-{run_attempt}.zip")

        for asset in assets:
            name = str(asset.get("name", ""))
            is_state_asset = name.startswith(prefix) and (
                name.endswith(".zip") or name.endswith(".json")
            )
            if not is_state_asset or name in keep_names:
                continue
            deleted = _run(
                [
                    "gh",
                    "release",
                    "delete-asset",
                    STATE_RELEASE_TAG,
                    name,
                    "--repo",
                    repository,
                    "--yes",
                ],
                check=False,
            )
            if deleted.returncode:
                print(
                    f"Warning: could not remove superseded Mode 1 state asset {name}: "
                    f"{(deleted.stderr or deleted.stdout).strip()}",
                    file=sys.stderr,
                )
    except Exception as exc:
        print(f"Warning: Mode 1 state asset cleanup failed: {exc}", file=sys.stderr)


def _restore_latest_release_state(
    *,
    repository: str,
    backend: str,
    working_dir: Path,
    snowflake_credentials: str | None = None,
) -> dict[str, Any] | None:
    """Restore the newest valid checkpoint, falling back to the preceding one."""
    backend = _backend(backend)
    assets = _release_assets(repository)
    prefix = f"toronto-mode1-state-{backend}-"
    asset_names = {str(asset.get("name", "")) for asset in assets}
    backend_state_assets = {
        name
        for name in asset_names
        if name.startswith(prefix) and name.endswith((".json", ".zip"))
    }
    candidates = _release_pointer_candidates(assets, backend)
    if not candidates:
        if backend_state_assets:
            raise RuntimeError(
                "Mode 1 Release contains backend state assets but no complete "
                "pointer/bundle checkpoint; refusing a fresh Mode 2 bootstrap."
            )
        return None
    if backend == "snowflake" and not snowflake_credentials:
        raise ValueError(
            "SNOWFLAKE_DLT_CONNECTION is required to restore Snowflake Mode 1."
        )

    failures: list[str] = []
    working_dir = Path(working_dir).resolve()
    with tempfile.TemporaryDirectory(prefix="toronto-mode1-restore-") as temporary:
        download_root = Path(temporary)
        for run_id, run_attempt, pointer_name in reversed(candidates):
            attempt_dir = download_root / f"{run_id}-{run_attempt}"
            attempt_dir.mkdir()
            try:
                _run(
                    [
                        "gh",
                        "release",
                        "download",
                        STATE_RELEASE_TAG,
                        "--repo",
                        repository,
                        "--pattern",
                        pointer_name,
                        "--dir",
                        str(attempt_dir),
                    ]
                )
                pointer_path = attempt_dir / pointer_name
                if not pointer_path.is_file():
                    raise RuntimeError(
                        "Release pointer download did not create its file."
                    )
                pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
                if (
                    pointer.get("format_version") != STATE_FORMAT_VERSION
                    or pointer.get("backend") != backend
                    or not isinstance(pointer.get("sha256"), str)
                    or len(pointer["sha256"]) != 64
                    or any(
                        char not in "0123456789abcdef"
                        for char in pointer["sha256"].lower()
                    )
                ):
                    raise RuntimeError("Mode 1 Release pointer metadata is invalid.")
                expected_bundle = f"{prefix}{run_id}-{run_attempt}.zip"
                asset_name = str(pointer.get("asset_name", ""))
                if asset_name != expected_bundle:
                    raise RuntimeError(
                        "Mode 1 Release pointer does not reference its paired bundle asset."
                    )
                _run(
                    [
                        "gh",
                        "release",
                        "download",
                        STATE_RELEASE_TAG,
                        "--repo",
                        repository,
                        "--pattern",
                        asset_name,
                        "--dir",
                        str(attempt_dir),
                    ]
                )
                bundle = attempt_dir / asset_name
                if not bundle.is_file() or _sha256(bundle) != pointer["sha256"]:
                    raise RuntimeError("Mode 1 Release state asset checksum mismatch.")

                restored_dir = attempt_dir / "restored"
                manifest = restore_state_bundle(bundle, restored_dir)
                if manifest.get("generation_id") != pointer.get("generation_id"):
                    raise RuntimeError(
                        "Mode 1 Release pointer and bundle generation IDs differ."
                    )
                if backend == "snowflake":
                    expected_generation_id = _manifest_snowflake_generation(manifest)
                    if manifest.get("promotion_status") == "PROMOTED":
                        state = _canonical_snowflake_mode1_marker_state(
                            str(snowflake_credentials), expected_generation_id
                        )
                        if state != "PROMOTED":
                            raise RuntimeError(
                                "Promoted Snowflake Mode 1 checkpoint is not marked PROMOTED."
                            )
                    elif manifest.get("handoff_status") == "validated":
                        try:
                            _validate_snowflake_checkpoint_schema(
                                credentials=str(snowflake_credentials),
                                schema=str(manifest["candidate_schema"]),
                                expected_generation_id=expected_generation_id,
                            )
                        except Exception as stage_error:
                            # After SWAP, the previous RAW occupies the old stage name.
                            # Accept only the exact READY/PROMOTED marker in canonical RAW.
                            try:
                                _canonical_snowflake_mode1_marker_state(
                                    str(snowflake_credentials), expected_generation_id
                                )
                            except Exception:
                                raise stage_error from None
                    else:
                        _validate_snowflake_checkpoint_schema(
                            credentials=str(snowflake_credentials),
                            schema=str(manifest["candidate_schema"]),
                            expected_generation_id=expected_generation_id,
                        )

                if working_dir.exists():
                    if any(working_dir.iterdir()):
                        raise FileExistsError(
                            f"Mode 1 restore destination is not empty: {working_dir}"
                        )
                    working_dir.rmdir()
                working_dir.parent.mkdir(parents=True, exist_ok=True)
                os.replace(restored_dir, working_dir)
                return manifest
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                failures.append(f"{pointer_name}: {detail}")
                print(
                    f"Warning: rejecting Mode 1 checkpoint {pointer_name}; "
                    f"trying the preceding checkpoint: {detail}",
                    file=sys.stderr,
                )

    raise RuntimeError(
        "No valid retained Mode 1 checkpoint could be restored. " + " | ".join(failures)
    )


def _write_github_outputs(values: dict[str, str]) -> None:
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            if "\n" in key or "\r" in key or "\n" in value or "\r" in value:
                raise ValueError("GitHub step outputs must be single-line values.")
            stream.write(f"{key}={value}\n")


def prepare_state(args: argparse.Namespace) -> dict[str, Any]:
    backend = _backend(args.backend)
    working_dir = Path(args.working_dir).resolve()
    if working_dir.exists() and not any(working_dir.iterdir()):
        working_dir.rmdir()

    snowflake_credentials = (
        args.snowflake_credentials if backend == "snowflake" else None
    )
    if backend == "snowflake" and not snowflake_credentials:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required for Snowflake Mode 1.")
    rebootstrap_from_mode2 = bool(getattr(args, "rebootstrap_from_mode2", False))
    baseline_run_id = str(args.baseline_run_id or "").strip()
    baseline_attempt = str(args.baseline_attempt or "1").strip()
    if rebootstrap_from_mode2 and not baseline_run_id:
        raise ValueError(
            "Explicit Mode 2 rebootstrap requires baseline_run_id and baseline_attempt."
        )

    production_baseline = None
    production_baseline_path = getattr(args, "production_baseline", None)
    if production_baseline_path:
        try:
            production_baseline = json.loads(
                Path(production_baseline_path).read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read Mode 1 production baseline: {exc}") from exc
        if (
            not isinstance(production_baseline, dict)
            or production_baseline.get("backend") != backend
        ):
            raise RuntimeError(
                "Mode 1 production baseline backend does not match the selected backend."
            )

    manifest = None
    if not rebootstrap_from_mode2:
        manifest = _restore_latest_release_state(
            repository=args.repository,
            backend=backend,
            working_dir=working_dir,
            snowflake_credentials=snowflake_credentials,
        )

    retryable_checkpoint = bool(
        manifest is not None and manifest.get("handoff_status") == "retryable"
    )
    validated_checkpoint = bool(
        manifest is not None
        and manifest.get("handoff_status") == "validated"
        and manifest.get("promotion_status") != "PROMOTED"
    )
    production_already_promoted = False
    snowflake_swap_pending = False
    if retryable_checkpoint and production_baseline is not None:
        prior_baseline = manifest.get("production_baseline")
        if prior_baseline is None or prior_baseline != production_baseline:
            raise RuntimeError(
                "Production changed after a retryable Mode 1 source load, or its "
                "original baseline is missing. Refusing to continue the partial candidate."
            )
    elif validated_checkpoint:
        if production_baseline is None:
            raise RuntimeError(
                "Validated Mode 1 recovery requires a freshly captured production baseline."
            )
        production_already_promoted = _validated_checkpoint_is_already_promoted(
            manifest=manifest,
            observed_baseline=production_baseline,
            backend=backend,
            production_database=getattr(args, "production_database", None),
        )
        if backend == "snowflake":
            if not snowflake_credentials:
                raise ValueError(
                    "SNOWFLAKE_DLT_CONNECTION is required to verify a promoted candidate."
                )
            try:
                marker_state = _canonical_snowflake_mode1_marker_state(
                    snowflake_credentials, _manifest_snowflake_generation(manifest)
                )
            except RuntimeError:
                marker_state = None
            if marker_state is not None:
                production_already_promoted = marker_state == "PROMOTED"
                snowflake_swap_pending = marker_state == "READY"
        if (
            not production_already_promoted
            and not snowflake_swap_pending
            and production_baseline != manifest.get("production_baseline")
        ):
            raise RuntimeError(
                "Production matches neither the original baseline nor the validated "
                "Mode 1 candidate. Refusing promotion recovery."
            )
    elif manifest is not None and production_baseline is not None:
        manifest["production_baseline"] = production_baseline

    requested_generation_id = (
        f"{baseline_run_id}:{baseline_attempt}" if baseline_run_id else None
    )
    if (
        manifest is not None
        and requested_generation_id is not None
        and str(manifest.get("generation_id", "")) != requested_generation_id
    ):
        existing_generation_id = str(manifest.get("generation_id", "unknown"))
        raise RuntimeError(
            "Persisted Mode 1 state uses Mode 2 generation "
            f"{existing_generation_id}, but this dispatch requested "
            f"{requested_generation_id}. Refusing to reuse a stale baseline. "
            "Set rebootstrap_from_mode2=true to explicitly bootstrap from the "
            "requested accepted Mode 2 generation."
        )

    needs_initial_checkpoint = manifest is None
    candidate_generation_id = f"{args.run_id}:{args.run_attempt}"

    if manifest is not None:
        if backend == "snowflake":
            credentials = args.snowflake_credentials
            if validated_checkpoint:
                # The validated schema is durable in Snowflake. Keep it intact so
                # an interrupted promotion can be resumed without replaying it.
                new_schema = str(manifest["candidate_schema"])
                candidate_generation_id = _manifest_snowflake_generation(manifest)
            elif retryable_checkpoint:
                source_schema = str(manifest["candidate_schema"])
                new_schema = _clone_snowflake_schema(
                    credentials=credentials,
                    source_schema=source_schema,
                    run_id=args.run_id,
                    run_attempt=args.run_attempt,
                    expected_generation_id=_manifest_snowflake_generation(manifest),
                )
            elif manifest.get("promotion_status") == "PROMOTED":
                promoted_generation = _manifest_snowflake_generation(manifest)
                new_schema = _clone_snowflake_schema(
                    credentials=credentials,
                    source_schema="RAW",
                    run_id=args.run_id,
                    run_attempt=args.run_attempt,
                    expected_promoted_generation_id=promoted_generation,
                )
            else:
                # Initial Mode 1 state is a clone of the accepted Mode 2 RAW.
                mode2_run_id, _mode2_attempt = _generation_parts(
                    str(manifest["generation_id"])
                )
                new_schema = _clone_snowflake_schema(
                    credentials=credentials,
                    source_schema="RAW",
                    run_id=args.run_id,
                    run_attempt=args.run_attempt,
                    expected_mode2_run_id=mode2_run_id,
                )
            manifest["candidate_schema"] = new_schema
            manifest["candidate"] = f"TORONTO.{new_schema}"
            # Keep the original candidate generation even when the checkpoint is
            # finalized in a later workflow run.
            manifest["candidate_generation_id"] = candidate_generation_id
            if not validated_checkpoint:
                manifest["promotion_status"] = "READY"
        elif backend == "duckdb":
            if (
                not retryable_checkpoint
                and not validated_checkpoint
                and getattr(args, "production_database", None)
            ):
                candidate_path = working_dir / "candidate.duckdb"
                _replace_candidate_from_production(
                    Path(args.production_database), candidate_path
                )
                manifest["candidate_sha256"] = _sha256(candidate_path)
                manifest["candidate_generation_id"] = candidate_generation_id
            elif not (working_dir / "candidate.duckdb").is_file():
                raise FileNotFoundError("Restored Mode 1 DuckDB candidate is missing.")
            if not validated_checkpoint:
                manifest["promotion_status"] = "READY"

        manifest["prepared_run_id"] = str(args.run_id)
        manifest["prepared_run_attempt"] = str(args.run_attempt)
        if production_baseline is not None and not validated_checkpoint:
            manifest["production_baseline"] = production_baseline
        (working_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    else:
        if not args.baseline_run_id:
            raise RuntimeError(
                "No durable Mode 1 state exists. Supply the successful Mode 2 "
                "workflow run ID and attempt for the one-time bootstrap."
            )
        with tempfile.TemporaryDirectory(prefix="toronto-mode1-mode2-") as temporary:
            temporary = Path(temporary)
            artifact_dir = temporary / "artifact"
            archive_path = _download_mode2_artifact(
                repository=args.repository,
                backend=backend,
                run_id=args.baseline_run_id,
                run_attempt=args.baseline_attempt,
                destination=artifact_dir,
            )
            mode2_root = temporary / "mode2"
            _safe_extract_tar(archive_path, mode2_root)
            ledger_path = mode2_root / f"{backend}-ledger.sqlite"
            if not ledger_path.is_file():
                raise FileNotFoundError(
                    f"Mode 2 artifact is missing {ledger_path.name}."
                )
            generation_id = f"{args.baseline_run_id}:{args.baseline_attempt}"
            metadata = _mode2_generation(ledger_path, backend, generation_id)

            if backend == "duckdb":
                candidate_name = Path(str(metadata["candidate"])).name
                if not candidate_name.endswith(".duckdb"):
                    raise RuntimeError("Mode 2 DuckDB candidate path is invalid.")
                candidate_path = mode2_root / "duckdb" / candidate_name
                if not candidate_path.is_file():
                    raise FileNotFoundError(
                        "Mode 2 artifact is missing its candidate database: "
                        f"{candidate_name}"
                    )
                manifest = bootstrap_duckdb_working_state(
                    mode2_ledger_path=ledger_path,
                    mode2_candidate_path=candidate_path,
                    working_dir=working_dir,
                    generation_id=generation_id,
                )
                if getattr(args, "production_database", None):
                    _replace_candidate_from_production(
                        Path(args.production_database), working_dir / "candidate.duckdb"
                    )
                    manifest["candidate_sha256"] = _sha256(
                        working_dir / "candidate.duckdb"
                    )
            else:
                manifest = bootstrap_snowflake_working_state(
                    mode2_ledger_path=ledger_path,
                    working_dir=working_dir,
                    generation_id=generation_id,
                    credentials=args.snowflake_credentials,
                    run_id=args.run_id,
                    run_attempt=args.run_attempt,
                )
            manifest["prepared_run_id"] = str(args.run_id)
            manifest["prepared_run_attempt"] = str(args.run_attempt)
            manifest["candidate_generation_id"] = candidate_generation_id
            manifest["promotion_status"] = "READY"
            if production_baseline is not None:
                manifest["production_baseline"] = production_baseline
            (working_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )

    ledger_path = working_dir / "replay-ledger.sqlite"
    marker = ReplayLedger(ledger_path).bootstrap_record(backend)
    if marker is None or marker["generation_id"] != manifest["generation_id"]:
        raise RuntimeError(
            "Prepared Mode 1 ledger does not match its accepted baseline."
        )

    outputs = {
        "baseline_generation_id": str(manifest["generation_id"]),
        "baseline_snapshot_id": str(manifest["baseline_snapshot_id"]),
        "baseline_snapshot_count": str(manifest["baseline_snapshot_count"]),
        "ledger": str(ledger_path),
        "working_dir": str(working_dir),
        "needs_initial_checkpoint": "true" if needs_initial_checkpoint else "false",
        "needs_promotion": "true" if validated_checkpoint else "false",
        "production_already_promoted": "true"
        if production_already_promoted
        else "false",
        "snowflake_swap_pending": "true" if snowflake_swap_pending else "false",
        "candidate_generation_id": str(
            manifest.get("candidate_generation_id", candidate_generation_id)
        ),
    }
    if backend == "duckdb":
        candidate = working_dir / "candidate.duckdb"
        if not candidate.is_file():
            raise FileNotFoundError("Prepared Mode 1 DuckDB candidate is missing.")
        outputs["database"] = str(candidate)
    else:
        schema = str(manifest["candidate_schema"])
        outputs["schema"] = schema
        outputs["candidate"] = f"TORONTO.{schema}"

    if production_baseline is not None:
        if retryable_checkpoint or validated_checkpoint:
            counts = manifest.get("quarantine_baseline_counts")
            snapshot_ids = manifest.get("quarantine_baseline_snapshot_ids")
            if not isinstance(counts, dict) or not isinstance(snapshot_ids, list):
                raise RuntimeError(
                    "Retryable Mode 1 state is missing its original quarantine "
                    "baseline."
                )
        else:
            counts, snapshot_ids = _candidate_quarantine_baseline(
                backend=backend,
                working_dir=working_dir,
                schema=(
                    str(manifest.get("candidate_schema", ""))
                    if backend == "snowflake"
                    else None
                ),
                credentials=snowflake_credentials,
            )
            manifest["quarantine_baseline_counts"] = counts
            manifest["quarantine_baseline_snapshot_ids"] = snapshot_ids
        outputs["quarantine_baseline_counts"] = json.dumps(counts, sort_keys=True)
        outputs["quarantine_baseline_snapshot_ids"] = json.dumps(
            snapshot_ids, separators=(",", ":")
        )
        (working_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    _write_github_outputs(outputs)
    return {**manifest, **outputs}


def _validate_handoff_summary(
    summary_path: Path, backend: str, manifest: dict[str, Any], working_dir: Path
) -> dict[str, Any]:
    summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    if (
        summary.get("backend") != backend
        or summary.get("mode") != "incremental"
        or summary.get("status") not in {"passed", "failed"}
    ):
        raise RuntimeError("Mode 1 state requires an incremental replay report.")
    history = summary.get("history_validation") or {}
    if history.get("status") != "passed":
        raise RuntimeError(
            "Mode 1 state cannot be checkpointed unless candidate SCD2 validation passed."
        )
    source_statuses = {
        source_name: (summary.get("sources", {}).get(source_name) or {}).get("status")
        for source_name in ("active_permits", "cleared_permits")
    }
    if any(status not in {"passed", "failed"} for status in source_statuses.values()):
        raise RuntimeError("Mode 1 report has invalid source-stream status evidence.")
    all_sources_passed = all(status == "passed" for status in source_statuses.values())
    if all_sources_passed != (summary.get("status") == "passed"):
        raise RuntimeError("Mode 1 overall and per-source replay statuses disagree.")
    manifest["handoff_status"] = "validated" if all_sources_passed else "retryable"

    if backend == "duckdb":
        expected = (working_dir / "candidate.duckdb").resolve()
        if Path(str(summary.get("candidate", ""))).resolve() != expected:
            raise RuntimeError(
                "Mode 1 summary candidate differs from the working database."
            )
        manifest["candidate_sha256"] = _sha256(expected)
    else:
        expected = f"TORONTO.{manifest['candidate_schema']}"
        if summary.get("candidate") != expected:
            raise RuntimeError(
                "Mode 1 summary candidate differs from the working Snowflake schema."
            )

    marker = ReplayLedger(working_dir / "replay-ledger.sqlite").bootstrap_record(
        backend
    )
    if (
        marker is None
        or marker["generation_id"] != manifest.get("generation_id")
        or marker["candidate"] != manifest.get("mode2_candidate")
        or int(marker["selected_snapshot_count"])
        != int(manifest.get("selected_snapshot_count", 0))
    ):
        raise RuntimeError(
            "Mode 1 handoff ledger has lost its accepted baseline identity."
        )
    return summary


def _validate_initial_baseline_checkpoint(
    *, backend: str, working_dir: Path, manifest: dict[str, Any]
) -> dict[str, Any]:
    """Verify that a first-run checkpoint is still exactly the accepted Mode 2 baseline."""
    backend = _backend(backend)
    ledger_path = Path(working_dir) / "replay-ledger.sqlite"
    if not ledger_path.is_file():
        raise FileNotFoundError(
            "Initial Mode 1 checkpoint is missing its working ledger."
        )
    ledger = ReplayLedger(ledger_path)
    marker = ledger.bootstrap_record(backend)
    if (
        marker is None
        or marker["generation_id"] != manifest.get("generation_id")
        or marker["candidate"] != manifest.get("mode2_candidate")
        or int(marker["selected_snapshot_count"])
        != int(manifest.get("selected_snapshot_count", 0))
    ):
        raise RuntimeError(
            "Initial Mode 1 checkpoint lost its accepted Mode 2 ledger identity."
        )

    with closing(
        sqlite3.connect(f"{ledger_path.as_uri()}?mode=ro", uri=True)
    ) as connection:
        rows = connection.execute(
            """SELECT snapshot_id, source_name, status
               FROM replay_loads WHERE backend = ?
               ORDER BY snapshot_id, source_name""",
            (backend,),
        ).fetchall()
    selected_count = int(manifest["selected_snapshot_count"])
    expected_sources = {"active_permits", "cleared_permits"}
    snapshots: dict[str, set[str]] = {}
    for snapshot_id, source_name, status in rows:
        if str(status) != "succeeded" or str(source_name) not in expected_sources:
            raise RuntimeError(
                "Initial Mode 1 checkpoint contains non-baseline source evidence."
            )
        snapshots.setdefault(str(snapshot_id), set()).add(str(source_name))
    if (
        len(snapshots) != selected_count
        or any(sources != expected_sources for sources in snapshots.values())
        or len(rows) != selected_count * 2
    ):
        raise RuntimeError(
            "Initial Mode 1 checkpoint ledger is not an unchanged complete baseline."
        )

    if backend == "duckdb":
        candidate = Path(working_dir) / "candidate.duckdb"
        if not candidate.is_file():
            raise FileNotFoundError(
                "Initial Mode 1 checkpoint is missing candidate.duckdb."
            )
        expected_hash = manifest.get("candidate_sha256")
        if not expected_hash or _sha256(candidate) != expected_hash:
            raise RuntimeError(
                "Initial Mode 1 DuckDB candidate changed before its baseline checkpoint."
            )
    else:
        schema = str(manifest.get("candidate_schema", "")).upper()
        run_id = str(manifest.get("prepared_run_id", ""))
        run_attempt = str(manifest.get("prepared_run_attempt", ""))
        if schema != f"RAW_STAGE_{run_id}_{run_attempt}":
            raise RuntimeError(
                "Initial Mode 1 Snowflake schema does not match its bootstrap run."
            )
        from scripts.replay_targets import (
            INCREMENTAL_SNOWFLAKE_SCHEMA_RE,
            _validate_snowflake_schema_owner,
        )
        from scripts.snowflake_operations import require_snowflake_operations_enabled

        require_snowflake_operations_enabled("Mode 1 baseline checkpoint validation")
        credentials = os.getenv("SNOWFLAKE_DLT_CONNECTION", "")
        if not credentials:
            raise ValueError(
                "SNOWFLAKE_DLT_CONNECTION is required for a Snowflake baseline checkpoint."
            )
        if not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(schema):
            raise RuntimeError(
                "Initial Mode 1 Snowflake candidate schema is not run-scoped."
            )
        connection, temporary = _snowflake_connection(credentials)
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s",
                    (schema,),
                )
                _validate_snowflake_schema_owner(schema, cursor.fetchone())
                cursor.execute(
                    f'SELECT GENERATION_ID, PROMOTION_STATE FROM TORONTO."{schema}".TORONTO_CANDIDATE_CONTROL'
                )
                control_rows = cursor.fetchall()
                if control_rows != [(f"{run_id}:{run_attempt}", "READY")]:
                    raise RuntimeError(
                        "Initial Mode 1 Snowflake candidate control marker is invalid."
                    )
            finally:
                cursor.close()
        finally:
            connection.close()
            temporary.cleanup()

    return {
        "backend": backend,
        "mode": "bootstrap",
        "status": "passed",
        "baseline_generation_id": str(manifest["generation_id"]),
        "history_validation": {
            "status": "passed",
            "basis": "accepted_mode2_full_history",
        },
    }


def create_state_bundle(
    *,
    backend: str,
    working_dir: Path,
    summary_path: Path | None,
    run_id: str,
    run_attempt: str,
    bootstrap_only: bool = False,
    promoted: bool = False,
    asset_run_attempt: str | None = None,
) -> tuple[Path, str, dict[str, Any]]:
    """Create a verified ZIP containing a baseline or incremental working state."""
    backend = _backend(backend)
    working_dir = Path(working_dir).resolve()
    manifest_path = working_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("backend") != backend:
        raise RuntimeError("Mode 1 working-state manifest backend mismatch.")
    if bootstrap_only:
        if summary_path is not None:
            raise ValueError(
                "A bootstrap-only checkpoint must not receive an incremental summary."
            )
        summary = _validate_initial_baseline_checkpoint(
            backend=backend, working_dir=working_dir, manifest=manifest
        )
        manifest["handoff_status"] = "baseline"
    else:
        if summary_path is None:
            raise ValueError(
                "Incremental Mode 1 state publication requires a replay summary."
            )
        summary = _validate_handoff_summary(
            summary_path, backend, manifest, working_dir
        )

    manifest.update(
        {
            "last_attempt_run_id": str(run_id),
            "last_attempt_run_attempt": str(run_attempt),
            "last_attempt_at_utc": datetime.now(UTC).isoformat(),
        }
    )
    if manifest["handoff_status"] == "validated":
        manifest.update(
            {
                "last_successful_run_id": str(run_id),
                "last_successful_run_attempt": str(run_attempt),
                "last_successful_at_utc": datetime.now(UTC).isoformat(),
            }
        )
    if promoted:
        if bootstrap_only or manifest.get("handoff_status") != "validated":
            raise RuntimeError(
                "Only a fully validated incremental handoff can be marked PROMOTED."
            )
        if backend == "snowflake" and manifest.get("candidate_schema") is None:
            raise RuntimeError(
                "Promoted Snowflake checkpoint is missing its candidate schema."
            )
        manifest["promotion_status"] = "PROMOTED"
    elif not bootstrap_only:
        manifest["promotion_status"] = "READY"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary_target = working_dir / "summary.json"
    if bootstrap_only:
        summary_target.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    else:
        shutil.copy2(summary_path, summary_target)

    asset_attempt = _state_asset_attempt(run_attempt, asset_run_attempt)
    asset_name = f"toronto-mode1-state-{backend}-{run_id}-{asset_attempt}.zip"
    bundle = working_dir / asset_name
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.write(manifest_path, "manifest.json")
        archive.write(working_dir / "replay-ledger.sqlite", "replay-ledger.sqlite")
        archive.write(summary_target, "summary.json")
        if backend == "duckdb":
            archive.write(working_dir / "candidate.duckdb", "candidate.duckdb")
    with zipfile.ZipFile(bundle) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("Created Mode 1 state ZIP failed its integrity check.")
    return bundle, _sha256(bundle), manifest


def publish_state(args: argparse.Namespace) -> dict[str, Any]:
    backend = _backend(args.backend)
    if not str(args.run_id).isdecimal() or not str(args.run_attempt).isdecimal():
        raise ValueError("Mode 1 publication run ID and attempt must be numeric.")
    working_dir = Path(args.working_dir).resolve()
    bootstrap_only = bool(getattr(args, "bootstrap_only", False))
    bundle, checksum, manifest = create_state_bundle(
        backend=backend,
        working_dir=working_dir,
        summary_path=None if bootstrap_only else args.summary,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
        bootstrap_only=bootstrap_only,
        promoted=bool(getattr(args, "promoted", False)),
        asset_run_attempt=getattr(args, "asset_run_attempt", None),
    )

    view = _run(
        ["gh", "release", "view", STATE_RELEASE_TAG, "--repo", args.repository],
        check=False,
    )
    if view.returncode:
        create = _run(
            [
                "gh",
                "release",
                "create",
                STATE_RELEASE_TAG,
                "--repo",
                args.repository,
                "--title",
                "Toronto Housing Mode 1 working state",
                "--notes",
                "Versioned validated Mode 1 staging state. Raw snapshot archives remain immutable and separate.",
            ],
            check=False,
        )
        if create.returncode:
            recheck = _run(
                ["gh", "release", "view", STATE_RELEASE_TAG, "--repo", args.repository],
                check=False,
            )
            if recheck.returncode:
                raise RuntimeError(
                    create.stderr.strip() or "Unable to create Mode 1 state Release."
                )

    _run(
        [
            "gh",
            "release",
            "upload",
            STATE_RELEASE_TAG,
            str(bundle),
            "--repo",
            args.repository,
        ]
    )
    pointer = {
        "format_version": STATE_FORMAT_VERSION,
        "backend": backend,
        "generation_id": manifest["generation_id"],
        "asset_name": bundle.name,
        "sha256": checksum,
        "published_at_utc": datetime.now(UTC).isoformat(),
    }
    asset_attempt = _state_asset_attempt(
        args.run_attempt, getattr(args, "asset_run_attempt", None)
    )
    pointer_name = _release_pointer_name(backend, args.run_id, asset_attempt)
    pointer_path = working_dir / pointer_name
    pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _run(
        [
            "gh",
            "release",
            "upload",
            STATE_RELEASE_TAG,
            str(pointer_path),
            "--repo",
            args.repository,
        ]
    )
    _cleanup_old_release_assets(
        repository=args.repository,
        backend=backend,
        keep_bundle=bundle.name,
        keep_pointer=pointer_name,
    )
    return pointer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser(
        "prepare", help="Restore or bootstrap the backend working state."
    )
    prepare.add_argument("--backend", choices=("duckdb", "snowflake"), required=True)
    prepare.add_argument(
        "--repository",
        default=os.getenv(
            "GITHUB_REPOSITORY", "gagadanalyst/toronto-housing-bottleneck-analytics"
        ),
    )
    prepare.add_argument("--working-dir", type=Path, required=True)
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument("--run-attempt", required=True)
    prepare.add_argument("--baseline-run-id", default="")
    prepare.add_argument("--baseline-attempt", default="1")
    prepare.add_argument(
        "--rebootstrap-from-mode2",
        action="store_true",
        help=(
            "Ignore an existing Mode 1 Release checkpoint and explicitly "
            "bootstrap from the supplied accepted full-history Mode 2 run."
        ),
    )
    prepare.add_argument(
        "--snowflake-credentials", default=os.getenv("SNOWFLAKE_DLT_CONNECTION")
    )
    prepare.add_argument("--production-database", type=Path)
    prepare.add_argument("--production-baseline", type=Path)

    publish = commands.add_parser(
        "publish", help="Publish a validated backend state handoff."
    )
    publish.add_argument("--backend", choices=("duckdb", "snowflake"), required=True)
    publish.add_argument(
        "--repository",
        default=os.getenv(
            "GITHUB_REPOSITORY", "gagadanalyst/toronto-housing-bottleneck-analytics"
        ),
    )
    publish.add_argument("--working-dir", type=Path, required=True)
    publish.add_argument("--summary", type=Path)
    publish.add_argument("--bootstrap-only", action="store_true")
    publish.add_argument(
        "--promoted",
        action="store_true",
        help="Mark the checkpoint as promoted after the backend production promoter succeeds.",
    )
    publish.add_argument("--run-id", required=True)
    publish.add_argument("--run-attempt", required=True)
    publish.add_argument(
        "--asset-run-attempt",
        help="Optional numeric suffix for multiple immutable checkpoints in one workflow run.",
    )

    discard = commands.add_parser(
        "discard", help="Drop an unused run-scoped Snowflake candidate schema."
    )
    discard.add_argument("--backend", choices=("snowflake",), required=True)
    discard.add_argument("--schema", required=True)
    discard.add_argument("--generation-id", required=True)
    discard.add_argument(
        "--snowflake-credentials", default=os.getenv("SNOWFLAKE_DLT_CONNECTION")
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "prepare":
            result = prepare_state(args)
        elif args.command == "publish":
            result = publish_state(args)
        else:
            if not args.snowflake_credentials:
                raise ValueError(
                    "SNOWFLAKE_DLT_CONNECTION is required for Snowflake Mode 1."
                )
            discard_empty_snowflake_candidate(
                credentials=args.snowflake_credentials,
                schema=args.schema,
                generation_id=args.generation_id,
            )
            result = {"status": "discarded", "schema": args.schema}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"Mode 1 state failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
