"""Promote an explicitly approved full-history Snapshot Loader rebuild into TORONTO.RAW.

This is a Mode 2 promotion path. It accepts only a run-scoped isolated rebuild
schema and never promotes a partial/N-snapshot/date-range candidate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from scripts.replay_targets import (
    REBUILD_SNOWFLAKE_SCHEMA_RE,
    SnowflakeReplayTarget,
    _validate_snowflake_account_context,
    validation_queries,
)
from scripts.snowflake_candidate_utils import decode_state, rebase_pipeline_state

TABLES = {
    "RAW_ACTIVE_PERMITS": ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
    "RAW_CLEARED_PERMITS": ("PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"),
}
REQUIRED_TABLES = {
    *TABLES,
    "TORONTO_SNAPSHOT_LEDGER",
    "_DLT_LOADS",
    "_DLT_VERSION",
    "_DLT_PIPELINE_STATE",
}
EXPECTED_PIPELINES = {
    "toronto_active_permits_scd2",
    "toronto_cleared_permits_scd2",
}


def _connection(uri: str):
    import snowflake.connector

    options, key_bytes, passphrase = SnowflakeReplayTarget._parse_credentials(uri)
    temp = tempfile.TemporaryDirectory(prefix="toronto-mode2-promote-")
    key_path = Path(temp.name) / "snowflake_key.p8"
    key_path.write_bytes(key_bytes)
    key_path.chmod(0o600)
    values = {**options, "private_key_file": str(key_path), "schema": "RAW"}
    if passphrase:
        values["private_key_file_pwd"] = passphrase
    try:
        connection = snowflake.connector.connect(**values)
    except Exception:
        temp.cleanup()
        raise
    return connection, temp


def _table_names(cursor, schema: str) -> set[str]:
    cursor.execute(
        "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s",
        (schema,),
    )
    return {str(row[0]).upper() for row in cursor.fetchall()}


def _assert_schema_owner(cursor, schema: str) -> None:
    cursor.execute(
        "SELECT SCHEMA_OWNER FROM TORONTO.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s",
        (schema,),
    )
    row = cursor.fetchone()
    if row is None or str(row[0]).upper() != "TORONTO_INGEST_ROLE":
        raise RuntimeError(
            f"TORONTO_INGEST_ROLE must own TORONTO.{schema}; observed={row!r}."
        )


def _expected_ledger_from_summary(
    path: Path, run_id: str
) -> dict[str, dict[str, object]]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot read Snowflake rebuild summary {path}: {exc}"
        ) from exc
    if not isinstance(report, dict):
        raise ValueError("Snowflake rebuild summary must be an object.")
    candidate = str(report.get("candidate", ""))
    if (
        report.get("backend") != "snowflake"
        or report.get("mode") != "rebuild"
        or report.get("history_scope") != "full"
        or report.get("full_history") is not True
        or report.get("explicit_promotion_required") is not True
        or report.get("warehouse_input_staging") != "not_used"
        or candidate != f"TORONTO.TORONTO_SNAPSHOT_REPLAY_{run_id}"
    ):
        raise ValueError(
            "Snowflake replay summary is not the requested full-history candidate."
        )
    selection = report.get("selection") or {}
    if any(
        selection.get(field) is not None for field in ("from_date", "to_date", "limit")
    ):
        raise ValueError(
            "Snowflake replay summary contains a partial rebuild selection."
        )
    discovered = report.get("snapshot_count_discovered")
    selected = report.get("snapshot_count_selected")
    snapshots = report.get("snapshots")
    if (
        type(discovered) is not int
        or type(selected) is not int
        or discovered < 1
        or selected != discovered
        or not isinstance(snapshots, list)
        or len(snapshots) != selected
    ):
        raise ValueError(
            "Snowflake replay summary does not prove complete archive coverage."
        )
    records = report.get("ledger_entries")
    if not isinstance(records, list) or len(records) != selected * 2:
        raise ValueError(
            "Snowflake summary must contain two source ledger entries per archive."
        )
    expected: dict[str, dict[str, object]] = {}
    for item in records:
        if not isinstance(item, dict):
            raise ValueError("Snowflake replay ledger entry is invalid.")
        source = str(item.get("source_name", ""))
        snapshot_id = str(item.get("snapshot_id", ""))
        key = f"{snapshot_id}|{source}"
        if (
            source not in {"active_permits", "cleared_permits"}
            or not snapshot_id
            or key in expected
        ):
            raise ValueError(
                f"Invalid or duplicate Snowflake replay ledger identity: {key}."
            )
        if item.get("status") != "succeeded":
            raise ValueError(f"Snowflake replay ledger entry is not succeeded: {key}.")
        fields = (
            "run_id",
            "run_attempt",
            "source_commit",
            "release_tag",
            "asset_name",
            "format_version",
            "archive_sha256",
            "order_timestamp",
            "quarantined_row_count",
            "quarantined_bad_row_count",
            "quarantined_business_key_count",
        )
        if any(item.get(field) in (None, "") for field in fields) or (
            "quarantine_sha256" not in item
        ):
            raise ValueError(
                f"Snowflake replay ledger entry lacks immutable identity "
                f"or quarantine evidence: {key}."
            )
        expected[key] = item
    wanted = {
        f"{item.get('snapshot_id')}|{source}"
        for item in snapshots
        for source in ("active_permits", "cleared_permits")
    }
    if set(expected) != wanted:
        raise ValueError(
            "Snowflake per-source ledger evidence does not cover the selected archive set."
        )
    return expected


def _assert_quarantine_reconciliation(
    cursor, ledger_rows, quarantine_rows, schema: str
) -> None:
    """Reconcile per-snapshot quarantine counts/digests without imposing a count threshold."""
    grouped = defaultdict(list)
    for row in quarantine_rows:
        key = (str(row[1]), str(row[2]))
        grouped[key].append(row)
        if not row[8] or not row[9] or not row[10]:
            raise RuntimeError(
                "Snowflake quarantine record is missing payload or immutable Parquet provenance."
            )
    ledger_pairs = {(str(row[0]), str(row[3])) for row in ledger_rows}
    orphaned = set(grouped) - ledger_pairs
    if orphaned:
        raise RuntimeError(
            f"Snowflake quarantine has orphaned ledger identities: {sorted(orphaned)}."
        )
    tables = {
        "active_permits": "RAW_ACTIVE_PERMITS",
        "cleared_permits": "RAW_CLEARED_PERMITS",
    }
    empty_digest = hashlib.sha256(b"").hexdigest()
    for entry in ledger_rows:
        key = (str(entry[0]), str(entry[3]))
        rows = grouped.get(key, [])
        total = len(rows)
        bad = sum(str(row[4]) != "identical_duplicate_record" for row in rows)
        conflict_keys = {
            str(row[5])
            for row in rows
            if str(row[4]) == "conflicting_duplicate_business_key"
            and row[5] is not None
        }
        digest = hashlib.sha256(
            "".join(str(row[0]) + "\n" for row in rows).encode("ascii")
        ).hexdigest()
        observed = (total, bad, len(conflict_keys), digest)
        expected = (
            int(entry[10] or 0),
            int(entry[11] or 0),
            int(entry[12] or 0),
            str(entry[13] or empty_digest),
        )
        if observed != expected:
            raise RuntimeError(
                f"Snowflake quarantine reconciliation failed for {key}: expected={expected}, observed={observed}."
            )
        for row in rows:
            if str(row[4]) == "identical_duplicate_record":
                continue
            table = tables[key[1]]
            # Quarantined rows must not appear in the clean SCD2 table at their source boundary.
            cursor.execute(
                f'SELECT COUNT(*) FROM TORONTO."{schema}"."{table}" WHERE ROW_HASH=%s AND _DLT_VALID_FROM=%s',
                (str(row[6]), row[7]),
            )
            if int(cursor.fetchone()[0] or 0):
                raise RuntimeError(
                    f"Quarantined row appears in clean SCD2 history for {key}."
                )


def _validate_schema(
    cursor,
    schema: str,
    expected_snapshots: int,
    expected_ledger: dict[str, dict[str, object]] | None = None,
) -> dict[str, dict[str, int]]:
    tables = _table_names(cursor, schema)
    missing = REQUIRED_TABLES - tables
    if missing:
        raise RuntimeError(
            f"TORONTO.{schema} is missing required tables: {sorted(missing)}."
        )

    metrics: dict[str, dict[str, int]] = {}
    for table, keys in TABLES.items():
        qualified = f'TORONTO."{schema}"."{table}"'
        checks = validation_queries(qualified, keys, snowflake=True)
        cursor.execute(checks["counts"])
        total, current, expired = (int(value or 0) for value in cursor.fetchone())
        source_metrics = {"total": total, "current": current, "expired": expired}
        for check, name in (
            ("invalid", "invalid_intervals"),
            ("overlap", "overlapping_intervals"),
            ("duplicate", "duplicate_current_business_keys"),
            ("null_keys", "null_business_keys"),
        ):
            cursor.execute(checks[check])
            source_metrics[name] = int(cursor.fetchone()[0] or 0)
        violations = {
            key: value
            for key, value in source_metrics.items()
            if key
            in {
                "invalid_intervals",
                "overlapping_intervals",
                "duplicate_current_business_keys",
                "null_business_keys",
            }
            and value != 0
        }
        if total <= 0 or total != current + expired or current <= 0 or violations:
            raise RuntimeError(
                f"SCD2 validation failed for TORONTO.{schema}.{table}: {source_metrics}."
            )
        metrics[table] = source_metrics

    cursor.execute(
        f'''SELECT SNAPSHOT_ID, SOURCE_RUN_ID, SOURCE_RUN_ATTEMPT, SOURCE_NAME, STATUS,
            SOURCE_COMMIT, RELEASE_TAG, ASSET_NAME, FORMAT_VERSION, ARCHIVE_SHA256,
            QUARANTINED_ROW_COUNT, QUARANTINED_BAD_ROW_COUNT,
            QUARANTINED_BUSINESS_KEY_COUNT, QUARANTINE_SHA256
            FROM TORONTO."{schema}".TORONTO_SNAPSHOT_LEDGER
            ORDER BY SNAPSHOT_ID, SOURCE_NAME'''
    )
    ledger_rows = cursor.fetchall()
    expected_rows = expected_snapshots * 2
    if len(ledger_rows) != expected_rows:
        raise RuntimeError(
            f"TORONTO.{schema} snapshot ledger has {len(ledger_rows)} entries; expected {expected_rows}."
        )
    observed_keys = set()
    for row in ledger_rows:
        snapshot_id, run_id, attempt, source_name = (
            str(row[0]),
            str(row[1]),
            int(row[2]),
            str(row[3]),
        )
        key = f"{snapshot_id}|{source_name}"
        if key in observed_keys or source_name not in {
            "active_permits",
            "cleared_permits",
        }:
            raise RuntimeError(
                f"Duplicate or unexpected Snowflake snapshot ledger identity: {key}."
            )
        observed_keys.add(key)
        if str(row[4]).lower() != "succeeded":
            raise RuntimeError(
                f"Snowflake snapshot ledger entry is not succeeded: {key}."
            )
        if expected_ledger is not None:
            item = expected_ledger.get(key)
            if item is None:
                raise RuntimeError(f"Unexpected Snowflake RAW ledger entry: {key}.")
            comparisons = (
                (run_id, str(item["run_id"])),
                (attempt, int(item["run_attempt"])),
                (str(row[5]), str(item["source_commit"])),
                (str(row[6]), str(item["release_tag"])),
                (str(row[7]), str(item["asset_name"])),
                (int(row[8]), int(item["format_version"])),
                (str(row[9]), str(item["archive_sha256"])),
                (int(row[10] or 0), int(item["quarantined_row_count"])),
                (
                    int(row[11] or 0),
                    int(item["quarantined_bad_row_count"]),
                ),
                (
                    int(row[12] or 0),
                    int(item["quarantined_business_key_count"]),
                ),
                (
                    str(row[13]) if row[13] is not None else None,
                    str(item["quarantine_sha256"])
                    if item["quarantine_sha256"] is not None
                    else None,
                ),
            )
            if any(actual != wanted for actual, wanted in comparisons):
                raise RuntimeError(
                    f"Snowflake RAW immutable provenance differs from replay evidence for {key}."
                )
    if expected_ledger is not None and observed_keys != set(expected_ledger):
        raise RuntimeError(
            "Snowflake RAW ledger does not exactly match replay evidence."
        )
    if len({str(row[0]) for row in ledger_rows}) != expected_snapshots or {
        str(row[3]) for row in ledger_rows
    } != {"active_permits", "cleared_permits"}:
        raise RuntimeError(
            "Snowflake RAW ledger does not contain both sources for every expected snapshot."
        )

    cursor.execute(
        "SELECT TABLE_NAME FROM TORONTO.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='TORONTO_SCD2_QUARANTINE'",
        (schema,),
    )
    has_quarantine = cursor.fetchone() is not None
    quarantine_rows = []
    if has_quarantine:
        cursor.execute(
            f'''SELECT QUARANTINE_ID, SNAPSHOT_ID, SOURCE_NAME, ROW_ORDINAL, REASON,
                BUSINESS_KEY_JSON, ROW_HASH, EXTRACTED_AT_UTC, RAW_RECORD_JSON,
                PARQUET_SHA256, ASSET_NAME
                FROM TORONTO."{schema}".TORONTO_SCD2_QUARANTINE
                ORDER BY SNAPSHOT_ID, SOURCE_NAME, ROW_ORDINAL, REASON'''
        )
        quarantine_rows = cursor.fetchall()
    _assert_quarantine_reconciliation(cursor, ledger_rows, quarantine_rows, schema)

    cursor.execute(
        f'''SELECT PIPELINE_NAME, STATE FROM TORONTO."{schema}"._DLT_PIPELINE_STATE'''
    )
    states = cursor.fetchall()
    observed_pipelines = {str(row[0]).lower() for row in states}
    if observed_pipelines != EXPECTED_PIPELINES:
        raise RuntimeError(
            f"Candidate DLT pipeline state is incomplete: {sorted(observed_pipelines)}."
        )
    allowed_datasets = {schema.upper(), "RAW"}
    for pipeline_value, state_value in states:
        pipeline = str(pipeline_value).lower()
        payload = decode_state(state_value, pipeline)
        if str(payload.get("pipeline_name", "")).lower() != pipeline:
            raise RuntimeError(f"DLT pipeline state name mismatch for {pipeline}.")
        if str(payload.get("dataset_name", "")).upper() not in allowed_datasets:
            raise RuntimeError(
                f"DLT pipeline state for {pipeline} points to an unexpected dataset."
            )
        if str(payload.get("default_schema_name", "")).lower() != pipeline:
            raise RuntimeError(f"DLT default schema name mismatch for {pipeline}.")
        schemas = payload.get("schema_names")
        if not isinstance(schemas, list) or pipeline not in {
            str(item).lower() for item in schemas
        }:
            raise RuntimeError(f"DLT schema_names does not contain {pipeline}.")
    return metrics


def promote(
    uri: str,
    source_schema: str,
    expected_run_id: str,
    expected_snapshots: int,
    *,
    confirmed: bool,
    summary_path: Path | None = None,
    expected_ledger: dict[str, dict[str, object]] | None = None,
) -> None:
    from scripts.snowflake_operations import require_snowflake_operations_enabled

    require_snowflake_operations_enabled("Promoting a Snowflake full-history rebuild")
    source_schema = source_schema.upper()
    if not REBUILD_SNOWFLAKE_SCHEMA_RE.fullmatch(source_schema):
        raise ValueError(f"Unsafe Mode 2 candidate schema: {source_schema!r}.")
    if not re.fullmatch(r"[0-9]+", expected_run_id):
        raise ValueError("Source workflow run ID must contain digits only.")
    if source_schema != f"TORONTO_SNAPSHOT_REPLAY_{expected_run_id}":
        raise ValueError(
            "Candidate schema does not belong to the authorized source workflow run."
        )
    if expected_snapshots < 1:
        raise ValueError("Full rebuild promotion requires at least one snapshot.")
    if not confirmed:
        raise PermissionError(
            "Explicit full-history promotion confirmation is required."
        )
    if summary_path is None:
        raise ValueError(
            "The full-history replay summary is required for provenance validation."
        )
    expected_ledger = _expected_ledger_from_summary(summary_path, expected_run_id)
    if len(expected_ledger) != expected_snapshots * 2:
        raise ValueError(
            "Expected snapshot count differs from full-history summary ledger size."
        )

    connection, temporary = _connection(uri)
    cursor = connection.cursor()
    swapped = False
    try:
        cursor.execute(
            "SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), "
            "CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME()"
        )
        _validate_snowflake_account_context(*cursor.fetchone())
        cursor.execute("SELECT CURRENT_SCHEMA()")
        if str(cursor.fetchone()[0]).upper() != "RAW":
            raise RuntimeError(
                "Snowflake Mode 2 promotion must start in TORONTO.RAW context."
            )
        _assert_schema_owner(cursor, "RAW")
        _assert_schema_owner(cursor, source_schema)

        # Idempotent retry after a completed prior promotion.
        raw_tables = _table_names(cursor, "RAW")
        if "TORONTO_CANDIDATE_CONTROL" in raw_tables:
            cursor.execute(
                "SELECT GENERATION_ID, PROMOTION_STATE FROM TORONTO.RAW.TORONTO_CANDIDATE_CONTROL"
            )
            control = cursor.fetchall()
            if control == [(f"rebuild:{expected_run_id}", "PROMOTED")]:
                _validate_schema(cursor, "RAW", expected_snapshots, expected_ledger)
                print(
                    "SNOWFLAKE MODE 2 PROMOTION ALREADY COMPLETED; canonical RAW is verified."
                )
                return

        _validate_schema(cursor, source_schema, expected_snapshots, expected_ledger)
        cursor.execute(
            f'''CREATE TABLE IF NOT EXISTS TORONTO."{source_schema}".TORONTO_CANDIDATE_CONTROL (
                GENERATION_ID VARCHAR NOT NULL,
                PROMOTION_STATE VARCHAR NOT NULL,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )'''
        )
        cursor.execute(
            f'DELETE FROM TORONTO."{source_schema}".TORONTO_CANDIDATE_CONTROL'
        )
        cursor.execute(
            f'''INSERT INTO TORONTO."{source_schema}".TORONTO_CANDIDATE_CONTROL
                (GENERATION_ID, PROMOTION_STATE) VALUES (%s, 'READY')''',
            (f"rebuild:{expected_run_id}",),
        )
        cursor.execute(
            f'GRANT USAGE ON SCHEMA TORONTO."{source_schema}" TO ROLE TORONTO_DBT_ROLE'
        )
        cursor.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA TORONTO."{source_schema}" TO ROLE TORONTO_DBT_ROLE'
        )
        cursor.execute(f'ALTER SCHEMA TORONTO.RAW SWAP WITH TORONTO."{source_schema}"')
        swapped = True

        # DLT state was created against the isolated candidate schema. After SWAP,
        # the same data is named RAW and its persisted dataset reference must follow.
        updated = rebase_pipeline_state(cursor, "RAW", source_schema, "RAW")
        cursor.execute(
            "UPDATE TORONTO.RAW.TORONTO_CANDIDATE_CONTROL "
            "SET PROMOTION_STATE='PROMOTED', UPDATED_AT=CURRENT_TIMESTAMP() "
            "WHERE GENERATION_ID=%s",
            (f"rebuild:{expected_run_id}",),
        )
        if int(cursor.rowcount) != 1:
            raise RuntimeError("Snowflake Mode 2 promotion marker did not persist.")
        _validate_schema(cursor, "RAW", expected_snapshots, expected_ledger)
        _assert_schema_owner(cursor, "RAW")
        _assert_schema_owner(cursor, source_schema)
        connection.commit()
        print(
            "SNOWFLAKE MODE 2 PROMOTION PASSED: full-history candidate is now TORONTO.RAW; "
            f"previous RAW is retained as TORONTO.{source_schema}; DLT state rows rebased={updated}."
        )
    except Exception:
        if swapped:
            try:
                cursor.execute(
                    f'ALTER SCHEMA TORONTO.RAW SWAP WITH TORONTO."{source_schema}"'
                )
                # The new candidate's pipeline state was rebased to RAW; restore its
                # isolated dataset pointer before leaving the rejected candidate.
                rebase_pipeline_state(cursor, source_schema, "RAW", source_schema)
                connection.commit()
                print(
                    "Mode 2 promotion verification failed; previous TORONTO.RAW was restored."
                )
            except Exception as rollback_error:
                raise RuntimeError(
                    "Mode 2 promotion failed after schema SWAP and rollback also failed; "
                    f"inspect TORONTO.RAW and TORONTO.{source_schema}: {rollback_error}"
                ) from rollback_error
        else:
            connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
        temporary.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--expected-snapshots", required=True, type=int)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--confirm-full-history", action="store_true")
    args = parser.parse_args()
    uri = os.environ.get("SNOWFLAKE_DLT_CONNECTION", "")
    if not uri:
        raise ValueError("SNOWFLAKE_DLT_CONNECTION is required.")
    try:
        promote(
            uri,
            args.source_schema,
            args.source_run_id,
            args.expected_snapshots,
            confirmed=args.confirm_full_history,
            summary_path=args.summary,
        )
        return 0
    except Exception as exc:
        print(
            f"SNOWFLAKE MODE 2 PROMOTION FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
