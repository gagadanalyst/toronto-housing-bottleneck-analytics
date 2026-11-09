"""Validate and atomically swap a staged Snowflake RAW candidate into production."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import snowflake.connector

from toronto_housing_ingestion.quarantine_policy import quarantine_generation_breaches

try:
    from snowflake_candidate_utils import decode_state, rebase_pipeline_state
except ImportError:
    from scripts.snowflake_candidate_utils import decode_state, rebase_pipeline_state

SOURCES = {
    "active_permits": (
        "RAW_ACTIVE_PERMITS",
        ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
    ),
    "cleared_permits": (
        "RAW_CLEARED_PERMITS",
        ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
    ),
}
PIPELINES = {
    "toronto_active_permits_scd2",
    "toronto_cleared_permits_scd2",
}
REQUIRED_STAGE_TABLES = {
    "RAW_ACTIVE_PERMITS",
    "RAW_CLEARED_PERMITS",
    "TORONTO_SNAPSHOT_LEDGER",
    "TORONTO_SCD2_QUARANTINE",
    "TORONTO_CANDIDATE_CONTROL",
    "_DLT_LOADS",
    "_DLT_PIPELINE_STATE",
    "_DLT_VERSION",
}


def _options(uri: str, key_path: Path) -> dict[str, str]:
    parts = urlsplit(uri)
    params = parse_qs(parts.query)
    if parts.scheme != "snowflake" or not parts.hostname or not parts.username:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is invalid.")
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


def _assert_scd2(cursor, schema: str, table: str, keys: tuple[str, ...]) -> None:
    key_sql = ", ".join(f'"{key}"' for key in keys)
    invalid_key_predicate = " OR ".join(
        f'"{key}" IS NULL OR TRIM(CAST("{key}" AS VARCHAR)) = ' + "''" for key in keys
    )
    cursor.execute(
        f'''SELECT COUNT_IF(_DLT_VALID_FROM IS NULL),
                   COUNT_IF(
                       _DLT_VALID_TO IS NOT NULL
                       AND _DLT_VALID_TO < _DLT_VALID_FROM
                   ),
                   COUNT_IF({invalid_key_predicate})
            FROM TORONTO."{schema}".{table}'''
    )
    null_from, invalid_intervals, invalid_keys = cursor.fetchone()
    cursor.execute(
        f'''SELECT COUNT(*) FROM (
            SELECT {key_sql} FROM TORONTO."{schema}".{table}
            WHERE _DLT_VALID_TO IS NULL GROUP BY {key_sql} HAVING COUNT(*) > 1
        )'''
    )
    duplicate_current = cursor.fetchone()[0]
    cursor.execute(
        f'''WITH ordered AS (
            SELECT {key_sql}, _DLT_VALID_FROM, _DLT_VALID_TO,
                   LEAD(_DLT_VALID_FROM) OVER (
                       PARTITION BY {key_sql} ORDER BY _DLT_VALID_FROM
                   ) AS NEXT_FROM
            FROM TORONTO."{schema}".{table}
        )
        SELECT COUNT(*) FROM ordered
        WHERE _DLT_VALID_TO IS NOT NULL AND NEXT_FROM IS NOT NULL
          AND _DLT_VALID_TO > NEXT_FROM'''
    )
    overlaps = cursor.fetchone()[0]
    if any(
        int(value or 0)
        for value in (
            null_from,
            invalid_intervals,
            invalid_keys,
            duplicate_current,
            overlaps,
        )
    ):
        raise RuntimeError(
            f"SCD2 validation failed for TORONTO.{schema}.{table}: "
            f"null_valid_from={null_from}, invalid_intervals={invalid_intervals}, "
            f"invalid_keys={invalid_keys}, duplicate_current={duplicate_current}, "
            f"overlaps={overlaps}."
        )
    cursor.execute(
        f'''SELECT COUNT(*), COUNT_IF(_DLT_VALID_TO IS NULL)
            FROM TORONTO."{schema}".{table}'''
    )
    total, current = cursor.fetchone()
    if not total or not current:
        raise RuntimeError(f"Candidate table TORONTO.{schema}.{table} is empty.")
    print(
        f"Validated TORONTO.{schema}.{table}: total={int(total):,}, "
        f"current={int(current):,}; SCD2 keys and intervals are valid."
    )


def _control_generation(cursor, schema: str) -> tuple[str, str] | None:
    cursor.execute(
        f'''SELECT GENERATION_ID, PROMOTION_STATE
            FROM TORONTO."{schema}".TORONTO_CANDIDATE_CONTROL'''
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"TORONTO.{schema}.TORONTO_CANDIDATE_CONTROL must contain exactly one row; "
            f"found {len(rows)}."
        )
    return str(rows[0][0]), str(rows[0][1]).upper()


def _assert_quarantine_ledger(
    cursor,
    schema: str,
    baseline_counts: dict[str, int],
    baseline_snapshot_ids: set[str],
) -> None:
    cursor.execute(
        f'''SELECT SNAPSHOT_ID, SOURCE_NAME, QUARANTINED_ROW_COUNT,
                   QUARANTINED_BAD_ROW_COUNT, QUARANTINED_BUSINESS_KEY_COUNT,
                   QUARANTINE_SHA256, STATUS
            FROM TORONTO."{schema}".TORONTO_SNAPSHOT_LEDGER'''
    )
    ledger_rows = cursor.fetchall()
    if not ledger_rows:
        raise RuntimeError("Candidate snapshot ledger is empty.")

    cursor.execute(
        f'''SELECT QUARANTINE_ID, SNAPSHOT_ID, SOURCE_NAME, ROW_ORDINAL,
                   REASON, BUSINESS_KEY_JSON, ROW_HASH, EXTRACTED_AT_UTC,
                   RAW_RECORD_JSON, PARQUET_SHA256, ASSET_NAME
            FROM TORONTO."{schema}".TORONTO_SCD2_QUARANTINE
            ORDER BY SNAPSHOT_ID, SOURCE_NAME, ROW_ORDINAL, REASON'''
    )
    quarantine_rows = cursor.fetchall()

    ledger_sources: dict[str, set[str]] = {}
    ledger_pairs: set[tuple[str, str]] = set()
    for snapshot_id, source_name, *_rest in ledger_rows:
        snapshot_key = str(snapshot_id)
        source_key = str(source_name)
        ledger_sources.setdefault(snapshot_key, set()).add(source_key)
        ledger_pairs.add((snapshot_key, source_key))
    if len(ledger_pairs) != len(ledger_rows):
        raise RuntimeError(
            "Candidate snapshot ledger contains duplicate snapshot/source rows."
        )
    for snapshot_id, source_names in ledger_sources.items():
        if source_names != set(SOURCES):
            raise RuntimeError(
                "Candidate ledger does not contain exactly Active and Cleared for "
                f"{snapshot_id}: {sorted(source_names)}."
            )

    grouped: dict[tuple[str, str], list[tuple]] = {}
    for row in quarantine_rows:
        quarantine_id, snapshot_id, source_name = row[:3]
        grouped.setdefault((str(snapshot_id), str(source_name)), []).append(row)
    orphaned = set(grouped) - ledger_pairs
    if orphaned:
        raise RuntimeError(
            "Snowflake quarantine contains rows without matching ledger entries: "
            f"{sorted(orphaned)}."
        )

    bad_rows_by_source: dict[str, int] = {}
    contributing_snapshots: dict[str, list[str]] = {}
    for (
        snapshot_id,
        source_name,
        row_count,
        bad_count,
        key_count,
        digest,
        status,
    ) in ledger_rows:
        if str(status).lower() != "succeeded":
            raise RuntimeError(
                f"Candidate ledger contains unfinished source {snapshot_id}/{source_name}: {status}."
            )
        rows = grouped.get((str(snapshot_id), str(source_name)), [])
        observed_bad_count = sum(
            1 for row in rows if str(row[4]) != "identical_duplicate_record"
        )
        observed_key_count = len(
            {
                str(row[5])
                for row in rows
                if str(row[4]) == "conflicting_duplicate_business_key"
                and row[5] is not None
            }
        )
        if (
            int(row_count or 0) != len(rows)
            or int(bad_count or 0) != observed_bad_count
            or int(key_count or 0) != observed_key_count
        ):
            raise RuntimeError(
                f"Quarantine reconciliation failed for {snapshot_id}/{source_name}: "
                f"ledger={row_count}/{bad_count}/{key_count}, "
                f"table={len(rows)}/{observed_bad_count}/{observed_key_count}."
            )
        if digest:
            observed_digest = hashlib.sha256(
                "".join(f"{row[0]}\n" for row in rows).encode("ascii")
            ).hexdigest()
            if observed_digest != str(digest):
                raise RuntimeError(
                    f"Quarantine digest mismatch for {snapshot_id}/{source_name}."
                )

        # Check each incoming bad row is absent from clean SCD2 at its source
        # extraction boundary. Existing historical versions for that key remain.
        table = SOURCES[str(source_name)][0]
        for row in rows:
            if not row[8] or not row[9] or not row[10]:
                raise RuntimeError(
                    f"Quarantine row {row[0]} is missing its original payload "
                    "or immutable Parquet provenance."
                )
            reason = str(row[4])
            if reason == "identical_duplicate_record":
                continue
            row_hash, extracted_at = row[6], row[7]
            cursor.execute(
                f'''SELECT COUNT(*) FROM TORONTO."{schema}".{table}
                    WHERE ROW_HASH = %s AND _DLT_VALID_FROM = %s''',
                (row_hash, extracted_at),
            )
            loaded = int(cursor.fetchone()[0] or 0)
            if loaded:
                raise RuntimeError(
                    f"Quarantined {reason} row {row[0]} appears in clean "
                    f"TORONTO.{schema}.{table} at its source boundary."
                )

        if str(snapshot_id) not in baseline_snapshot_ids:
            bad_rows_by_source[str(source_name)] = bad_rows_by_source.get(
                str(source_name), 0
            ) + int(bad_count or 0)
            if int(bad_count or 0):
                contributing_snapshots.setdefault(str(source_name), []).append(
                    str(snapshot_id)
                )

    breaches = quarantine_generation_breaches(bad_rows_by_source, baseline_counts)
    for source_name, (bad_rows, baseline) in breaches.items():
        ratio = bad_rows / baseline if baseline else float("inf")
        raise RuntimeError(
            f"Quarantine threshold breached for {source_name} in this candidate "
            f"generation: {bad_rows}/{baseline} ({ratio:.2%}) >= 25%. "
            f"Contributing snapshots={contributing_snapshots.get(source_name, [])}. "
            "Candidate RAW was not promoted; its quarantine rows remain in the stage schema."
        )


def _validate_candidate(
    cursor, stage: str, baseline_counts, baseline_snapshot_ids
) -> None:
    cursor.execute(
        "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s",
        (stage,),
    )
    available = {str(row[0]).upper() for row in cursor.fetchall()}
    missing = REQUIRED_STAGE_TABLES - available
    if missing:
        raise RuntimeError(
            f"TORONTO.{stage} is missing required tables: {sorted(missing)}."
        )
    for _source_name, (table, keys) in SOURCES.items():
        _assert_scd2(cursor, stage, table, keys)
    _assert_quarantine_ledger(cursor, stage, baseline_counts, baseline_snapshot_ids)

    cursor.execute(
        f'''SELECT PIPELINE_NAME, STATE, VERSION, ENGINE_VERSION, VERSION_HASH, _DLT_LOAD_ID
            FROM TORONTO."{stage}"._DLT_PIPELINE_STATE'''
    )
    state_rows = cursor.fetchall()
    state_counts: dict[str, int] = {name: 0 for name in PIPELINES}
    for (
        pipeline_name,
        state_value,
        version,
        engine_version,
        version_hash,
        load_id,
    ) in state_rows:
        pipeline = str(pipeline_name).lower()
        if pipeline not in PIPELINES:
            raise RuntimeError(f"Unexpected DLT pipeline state row: {pipeline_name!r}.")
        payload = decode_state(state_value, pipeline)
        if str(payload.get("dataset_name", "")).upper() != stage:
            raise RuntimeError(
                f"DLT state for {pipeline} does not point to candidate {stage}."
            )
        if (
            int(version) != int(payload.get("_state_version", -1))
            or int(engine_version) != int(payload.get("_state_engine_version", -1))
            or str(version_hash) != str(payload.get("_version_hash", ""))
            or not load_id
        ):
            raise RuntimeError(f"DLT state metadata does not reconcile for {pipeline}.")
        state_counts[pipeline] += 1
    if any(count == 0 for count in state_counts.values()):
        raise RuntimeError(
            f"Candidate DLT pipeline states are incomplete: {state_counts}."
        )

    cursor.execute(
        f'''SELECT SCHEMA_NAME, COUNT_IF(STATUS = 0)
            FROM TORONTO."{stage}"._DLT_LOADS GROUP BY SCHEMA_NAME'''
    )
    load_counts = {
        str(name).lower(): int(count or 0) for name, count in cursor.fetchall()
    }
    for pipeline in PIPELINES:
        if load_counts.get(pipeline, 0) < 1:
            raise RuntimeError(f"Candidate has no successful DLT loads for {pipeline}.")


def promote(
    uri: str,
    stage: str,
    generation: str,
    baseline_counts: dict[str, int],
    baseline_snapshot_ids: set[str],
) -> None:
    if not re.fullmatch(r"RAW_STAGE_[0-9]+_[0-9]+", stage):
        raise ValueError(f"Unsafe candidate schema name: {stage!r}.")
    expected_generation = ":".join(stage.removeprefix("RAW_STAGE_").split("_"))
    if generation != expected_generation:
        raise ValueError(
            f"Candidate generation mismatch: stage={stage}, generation={generation}."
        )

    with tempfile.TemporaryDirectory(prefix="toronto-raw-promote-") as temp:
        options = _options(uri, Path(temp) / "snowflake_key.p8")
        connection = snowflake.connector.connect(**options)
        cursor = connection.cursor()
        swapped = False
        try:
            cursor.execute(
                "SELECT SCHEMA_NAME, SCHEMA_OWNER "
                "FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME IN ('RAW', %s)",
                (stage,),
            )
            owners = {
                str(name).upper(): str(owner).upper()
                for name, owner in cursor.fetchall()
            }
            if (
                owners.get("RAW") != "TORONTO_INGEST_ROLE"
                or owners.get(stage) != "TORONTO_INGEST_ROLE"
            ):
                raise RuntimeError(
                    "TORONTO_INGEST_ROLE must own both RAW and the candidate before SWAP; "
                    f"observed={owners}."
                )

            # A prior invocation may have completed SWAP and then been interrupted
            # before the DLT state rebase. The generation marker travels with the
            # candidate schema, allowing this step to finish safely and idempotently.
            cursor.execute(
                "SELECT COUNT(*) FROM TORONTO.INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'TORONTO_CANDIDATE_CONTROL'"
            )
            if int(cursor.fetchone()[0] or 0):
                try:
                    current_generation, _current_state = _control_generation(
                        cursor, "RAW"
                    )
                except RuntimeError:
                    current_generation = ""
                if current_generation == generation:
                    # The generation marker is in canonical RAW, so the schema swap
                    # is already durable. Arm rollback before resuming state or checks.
                    swapped = True
                    state_counts = []
                    cursor.execute("SELECT STATE FROM TORONTO.RAW._DLT_PIPELINE_STATE")
                    for (state_value,) in cursor.fetchall():
                        payload = decode_state(state_value, "candidate-recovery")
                        state_counts.append(
                            str(payload.get("dataset_name", "")).upper()
                        )
                    if any(value == stage for value in state_counts):
                        updated = rebase_pipeline_state(cursor, "RAW", stage, "RAW")
                        connection.commit()
                    else:
                        updated = 0
                    cursor.execute(
                        "UPDATE TORONTO.RAW.TORONTO_CANDIDATE_CONTROL "
                        "SET PROMOTION_STATE = 'PROMOTED', UPDATED_AT = CURRENT_TIMESTAMP()"
                    )
                    connection.commit()
                    for _source_name, (table, keys) in SOURCES.items():
                        _assert_scd2(cursor, "RAW", table, keys)
                    _assert_quarantine_ledger(
                        cursor, "RAW", baseline_counts, baseline_snapshot_ids
                    )
                    cursor.execute(
                        "SELECT COUNT_IF(LOWER(STATUS) != 'succeeded') "
                        "FROM TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"
                    )
                    if int(cursor.fetchone()[0] or 0):
                        raise RuntimeError(
                            "Resumed RAW candidate has unfinished source ledger entries."
                        )
                    print(
                        "SNOWFLAKE RAW PROMOTION RESUMED: this generation was already "
                        f"swapped; completed {updated} DLT state rebase(s)."
                    )
                    return

            _validate_candidate(cursor, stage, baseline_counts, baseline_snapshot_ids)
            stage_generation, stage_state = _control_generation(cursor, stage)
            if stage_generation != generation or stage_state != "READY":
                raise RuntimeError(
                    f"Candidate control marker mismatch: observed={stage_generation}/{stage_state}, "
                    f"expected={generation}/READY."
                )

            # Schema-level grants are not inherited by CLONE; explicitly grant the
            # dbt role read access before the candidate becomes canonical RAW.
            cursor.execute(
                f'GRANT USAGE ON SCHEMA TORONTO."{stage}" TO ROLE TORONTO_DBT_ROLE'
            )
            for table, _keys in SOURCES.values():
                cursor.execute(
                    f'GRANT SELECT ON TABLE TORONTO."{stage}".{table} '
                    "TO ROLE TORONTO_DBT_ROLE"
                )
            cursor.execute(
                f'GRANT SELECT ON TABLE TORONTO."{stage}"._DLT_LOADS '
                "TO ROLE TORONTO_DBT_ROLE"
            )

            cursor.execute(f'ALTER SCHEMA TORONTO.RAW SWAP WITH TORONTO."{stage}"')
            swapped = True

            # Rebase after SWAP. If the process is interrupted between these
            # operations, the marker in RAW identifies the generation to resume.
            updated = rebase_pipeline_state(cursor, "RAW", stage, "RAW")
            cursor.execute(
                "UPDATE TORONTO.RAW.TORONTO_CANDIDATE_CONTROL "
                "SET PROMOTION_STATE = 'PROMOTED', UPDATED_AT = CURRENT_TIMESTAMP()"
            )
            connection.commit()

            for _source_name, (table, keys) in SOURCES.items():
                _assert_scd2(cursor, "RAW", table, keys)
            cursor.execute(
                "SELECT COUNT_IF(LOWER(STATUS) != 'succeeded') "
                "FROM TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER"
            )
            unfinished = int(cursor.fetchone()[0] or 0)
            if unfinished:
                raise RuntimeError(
                    f"Promoted RAW ledger contains {unfinished} unfinished source entries."
                )
            promoted_generation, promoted_state = _control_generation(cursor, "RAW")
            if promoted_generation != generation or promoted_state != "PROMOTED":
                raise RuntimeError("RAW promotion marker did not reach PROMOTED state.")
            print(
                "SNOWFLAKE RAW PROMOTION PASSED: candidate is now TORONTO.RAW; "
                f"previous RAW is retained as TORONTO.{stage}; DLT state rows rebased={updated}."
            )
        except Exception:
            if swapped:
                try:
                    cursor.execute(
                        f'ALTER SCHEMA TORONTO.RAW SWAP WITH TORONTO."{stage}"'
                    )
                    # Restore the old production schema first, then make the
                    # rejected candidate self-consistent even if the forward rebase
                    # only partially completed before verification failed.
                    rebase_pipeline_state(cursor, stage, "RAW", stage)
                    connection.commit()
                    print(
                        "RAW candidate promotion failed verification; previous production "
                        "RAW was restored and the candidate DLT state was returned to its stage dataset."
                    )
                except Exception as rollback_error:
                    raise RuntimeError(
                        "RAW promotion failed after SWAP and automatic rollback failed: "
                        f"{rollback_error}. Inspect TORONTO.RAW and TORONTO.{stage} before retrying."
                    ) from rollback_error
            else:
                connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage-schema",
        default=os.environ.get("TORONTO_SNOWFLAKE_STAGE_SCHEMA", ""),
    )
    parser.add_argument(
        "--generation",
        default=os.environ.get("TORONTO_SNOWFLAKE_STAGE_GENERATION", ""),
    )
    parser.add_argument(
        "--baseline-counts",
        default=os.environ.get("TORONTO_QUARANTINE_BASELINE_COUNTS", ""),
    )
    parser.add_argument(
        "--baseline-snapshot-ids",
        default=os.environ.get("TORONTO_QUARANTINE_BASELINE_SNAPSHOT_IDS", ""),
    )
    args = parser.parse_args()
    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required.")
    if not args.baseline_counts or not args.baseline_snapshot_ids:
        raise ValueError("Quarantine baseline counts and snapshot IDs are required.")
    promote(
        uri,
        args.stage_schema,
        args.generation,
        json.loads(args.baseline_counts),
        set(json.loads(args.baseline_snapshot_ids)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
