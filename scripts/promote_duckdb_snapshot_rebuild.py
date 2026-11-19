"""Validate and locally stage a full-history Mode 2 DuckDB rebuild for publication.

This command never writes to a remote warehouse. It verifies the isolated rebuild,
provenance ledger and quarantine records, proves dbt did not alter RAW state, then
atomically places the validated dbt-built candidate at the workflow publication path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.replay_targets import validation_queries

SOURCE_TABLES = {
    "active_permits": (
        "raw_active_permits",
        ("permit_num", "revision_num", "permit_type"),
    ),
    "cleared_permits": (
        "raw_cleared_permits",
        ("permit_num", "revision_num", "permit_type"),
    ),
}
REQUIRED_RAW_TABLES = {
    "raw_active_permits",
    "raw_cleared_permits",
    "toronto_snapshot_ledger",
    "toronto_scd2_quarantine",
    "_dlt_loads",
    "_dlt_version",
    "_dlt_pipeline_state",
}


def _read_report(
    path: Path, run_id: str
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("DuckDB rebuild summary must be a JSON object.")
    if (
        report.get("backend") != "duckdb"
        or report.get("mode") != "rebuild"
        or report.get("history_scope") != "full"
        or report.get("full_history") is not True
        or report.get("explicit_promotion_required") is not True
        or report.get("warehouse_input_staging") != "not_used"
    ):
        raise ValueError("DuckDB summary is not a full-history Mode 2 rebuild.")
    if any(
        report.get("selection", {}).get(key) is not None
        for key in ("from_date", "to_date", "limit")
    ):
        raise ValueError("DuckDB summary contains a partial rebuild selection.")
    snapshots = report.get("snapshots")
    count = report.get("snapshot_count_selected")
    if (
        not isinstance(snapshots, list)
        or type(count) is not int
        or count <= 0
        or len(snapshots) != count
        or report.get("snapshot_count_discovered") != count
    ):
        raise ValueError("DuckDB summary does not prove complete archive selection.")
    source_candidate = Path(str(report.get("candidate", ""))).resolve()
    expected_source = (
        Path("data/recovery/snapshot-replay/duckdb") / f"toronto-replay-{run_id}.duckdb"
    ).resolve()
    if source_candidate != expected_source:
        raise ValueError(
            "DuckDB summary candidate does not match the requested run-scoped rebuild path: "
            f"expected={expected_source}, observed={source_candidate}."
        )
    entries = report.get("ledger_entries")
    if not isinstance(entries, list) or len(entries) != count * 2:
        raise ValueError(
            "DuckDB replay summary must contain two source ledger entries per archive."
        )
    expected: dict[str, dict[str, Any]] = {}
    for item in entries:
        if not isinstance(item, dict):
            raise ValueError("DuckDB replay ledger entry is not an object.")
        snapshot_id = str(item.get("snapshot_id", ""))
        source = str(item.get("source_name", ""))
        if source not in SOURCE_TABLES or not snapshot_id:
            raise ValueError(
                "DuckDB replay ledger entry has an invalid source/snapshot identity."
            )
        key = f"{snapshot_id}|{source}"
        if key in expected:
            raise ValueError(f"Duplicate DuckDB replay ledger entry: {key}.")
        if item.get("status") != "succeeded":
            raise ValueError(f"DuckDB replay ledger entry is not succeeded: {key}.")
        required = (
            "format_version",
            "run_id",
            "run_attempt",
            "source_commit",
            "archive_sha256",
            "release_tag",
            "asset_name",
            "order_timestamp",
            "quarantined_row_count",
            "quarantined_bad_row_count",
            "quarantined_business_key_count",
        )
        if any(item.get(field) in (None, "") for field in required) or (
            "quarantine_sha256" not in item
        ):
            raise ValueError(
                f"DuckDB replay ledger entry lacks immutable provenance "
                f"or quarantine evidence: {key}."
            )
        if str(item["run_id"]) != str(
            next(
                (
                    s.get("run_id")
                    for s in snapshots
                    if s.get("snapshot_id") == snapshot_id
                ),
                "",
            )
        ):
            raise ValueError(
                f"DuckDB ledger run ID disagrees with selected archive {snapshot_id}."
            )
        expected[key] = item
    wanted = {
        f"{s.get('snapshot_id')}|{source}"
        for s in snapshots
        for source in SOURCE_TABLES
    }
    if set(expected) != wanted:
        raise ValueError(
            "DuckDB source ledger identities do not exactly cover selected archives."
        )
    return report, expected


def _json_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError, ValueError:
            pass
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def raw_schema_fingerprint(database: Path) -> str:
    """Hash every RAW table's schema and ordered contents, including DLT state."""
    import duckdb

    digest = hashlib.sha256()
    with duckdb.connect(str(database), read_only=True) as connection:
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE lower(table_schema)='raw' AND table_type='BASE TABLE' "
                "ORDER BY lower(table_name)"
            ).fetchall()
        ]
        for table in tables:
            safe = table.replace('"', '""')
            columns = [
                str(row[0])
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE lower(table_schema)='raw' AND table_name=? "
                    "ORDER BY ordinal_position",
                    [table],
                ).fetchall()
            ]
            digest.update(json.dumps([table, columns], separators=(",", ":")).encode())
            digest.update(b"\n")
            cursor = connection.execute(f' SELECT * FROM raw."{safe}" ORDER BY ALL')
            while rows := cursor.fetchmany(10_000):
                for row in rows:
                    normalized = [_json_value(value) for value in row]
                    digest.update(
                        json.dumps(
                            normalized, default=str, separators=(",", ":")
                        ).encode()
                    )
                    digest.update(b"\n")
    return digest.hexdigest()


def _validate_database(
    database: Path, expected: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    import duckdb

    if not database.is_file() or database.stat().st_size <= 0:
        raise FileNotFoundError(f"DuckDB candidate is missing or empty: {database}.")
    metrics: dict[str, Any] = {}
    with duckdb.connect(str(database), read_only=True) as connection:
        tables = {
            str(row[0]).lower()
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE lower(table_schema)='raw' AND table_type='BASE TABLE'"
            ).fetchall()
        }
        missing = REQUIRED_RAW_TABLES - tables
        if missing:
            raise RuntimeError(
                f"DuckDB rebuild is missing RAW tables: {sorted(missing)}."
            )

        ledger_rows = connection.execute(
            "SELECT snapshot_id, source_run_id, source_run_attempt, source_name, status, "
            "source_commit, release_tag, asset_name, format_version, archive_sha256, "
            "quarantined_row_count, quarantined_bad_row_count, "
            "quarantined_business_key_count, quarantine_sha256 "
            "FROM raw.toronto_snapshot_ledger ORDER BY snapshot_id, source_name"
        ).fetchall()
        actual_ledger: dict[str, tuple[Any, ...]] = {}
        for row in ledger_rows:
            snapshot_id, run_id, attempt, source_name = (
                str(row[0]),
                str(row[1]),
                int(row[2]),
                str(row[3]),
            )
            key = f"{snapshot_id}|{source_name}"
            if key in actual_ledger:
                raise RuntimeError(f"Duplicate RAW snapshot ledger entry: {key}.")
            actual_ledger[key] = row
            item = expected.get(key)
            if item is None:
                raise RuntimeError(f"Unexpected RAW snapshot ledger entry: {key}.")
            comparisons = (
                (run_id, str(item["run_id"])),
                (attempt, int(item["run_attempt"])),
                (str(row[4]).lower(), "succeeded"),
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
                    f"RAW ledger immutable provenance differs from summary for {key}."
                )
        if set(actual_ledger) != set(expected):
            raise RuntimeError(
                "RAW snapshot ledger does not exactly match the successful replay summary."
            )

        # Canonical pipeline names and dataset pointers must survive Mode 2 promotion,
        # otherwise the next routine load_scd2() invocation could start a new DLT history.
        from scripts.snowflake_candidate_utils import decode_state

        state_rows = connection.execute(
            "SELECT pipeline_name, state FROM raw._dlt_pipeline_state"
        ).fetchall()
        expected_pipelines = {
            "toronto_active_permits_scd2",
            "toronto_cleared_permits_scd2",
        }
        if {str(row[0]).lower() for row in state_rows} != expected_pipelines:
            raise RuntimeError(
                "DuckDB rebuild DLT pipeline state is not canonical and complete."
            )
        for pipeline_value, state_value in state_rows:
            pipeline = str(pipeline_value).lower()
            payload = decode_state(state_value, pipeline)
            if str(payload.get("pipeline_name", "")).lower() != pipeline:
                raise RuntimeError(
                    f"DuckDB DLT pipeline state name mismatch for {pipeline}."
                )
            if str(payload.get("dataset_name", "")).lower() != "raw":
                raise RuntimeError(
                    f"DuckDB DLT pipeline state for {pipeline} does not point to RAW."
                )
            if str(payload.get("default_schema_name", "")).lower() != pipeline:
                raise RuntimeError(
                    f"DuckDB DLT default schema mismatch for {pipeline}."
                )
            schemas = payload.get("schema_names")
            if not isinstance(schemas, list) or pipeline not in {
                str(item).lower() for item in schemas
            }:
                raise RuntimeError(
                    f"DuckDB DLT schema_names does not contain {pipeline}."
                )

        quarantine_rows = connection.execute(
            "SELECT quarantine_id, snapshot_id, source_name, row_ordinal, reason, "
            "business_key_json, row_hash, extracted_at_utc, raw_record_json, "
            "parquet_sha256, asset_name "
            "FROM raw.toronto_scd2_quarantine "
            "ORDER BY snapshot_id, source_name, row_ordinal, reason"
        ).fetchall()
        grouped: dict[tuple[str, str], list[tuple[Any, ...]]] = defaultdict(list)
        for row in quarantine_rows:
            grouped[(str(row[1]), str(row[2]))].append(row)
            if not row[8] or not row[9] or not row[10]:
                raise RuntimeError(
                    "A quarantine record is missing raw payload or immutable Parquet provenance."
                )
        ledger_by_key = {(str(row[0]), str(row[3])): row for row in ledger_rows}
        if set(grouped) - set(ledger_by_key):
            raise RuntimeError(
                "Quarantine contains rows without a matching snapshot ledger entry."
            )
        source_table_by_name = {
            source: table for source, (table, _keys) in SOURCE_TABLES.items()
        }
        for key, ledger in ledger_by_key.items():
            rows = grouped.get(key, [])
            total = len(rows)
            bad = sum(str(row[4]) != "identical_duplicate_record" for row in rows)
            conflict_keys = {
                str(row[5])
                for row in rows
                if str(row[4]) == "conflicting_duplicate_business_key"
                and row[5] is not None
            }
            digest = (
                hashlib.sha256(
                    "".join(str(row[0]) + "\n" for row in rows).encode("ascii")
                ).hexdigest()
                if rows
                else None
            )
            if (total, bad, len(conflict_keys), digest) != (
                int(ledger[10] or 0),
                int(ledger[11] or 0),
                int(ledger[12] or 0),
                str(ledger[13]) if ledger[13] is not None else None,
            ):
                raise RuntimeError(f"Quarantine reconciliation failed for {key}.")
            for row in rows:
                if str(row[4]) == "identical_duplicate_record":
                    continue
                table = source_table_by_name[key[1]]
                loaded = connection.execute(
                    f'SELECT COUNT(*) FROM raw."{table}" WHERE row_hash=? AND _dlt_valid_from=?',
                    [str(row[6]), row[7]],
                ).fetchone()[0]
                if int(loaded or 0):
                    raise RuntimeError(
                        f"Quarantined row appears in clean SCD2 history for {key}."
                    )

        for source_name, (table, keys) in SOURCE_TABLES.items():
            checks = validation_queries(f'raw."{table}"', keys, snowflake=False)
            cursor = connection.cursor()
            source_metrics = {}
            for query_name, field in (
                ("counts", "counts"),
                ("invalid", "invalid_intervals"),
                ("overlap", "overlapping_intervals"),
                ("duplicate", "duplicate_current_business_keys"),
                ("null_keys", "null_business_keys"),
            ):
                cursor.execute(checks[query_name])
                result = cursor.fetchone()
                if query_name == "counts":
                    total, current, expired = (int(value or 0) for value in result)
                    source_metrics.update(total=total, current=current, expired=expired)
                else:
                    source_metrics[field] = int(result[0] or 0)
            cursor.close()
            invalids = {
                key: val
                for key, val in source_metrics.items()
                if key
                in {
                    "invalid_intervals",
                    "overlapping_intervals",
                    "duplicate_current_business_keys",
                    "null_business_keys",
                }
                and val
            }
            if (
                source_metrics["total"] <= 0
                or source_metrics["current"] <= 0
                or source_metrics["total"]
                != source_metrics["current"] + source_metrics["expired"]
                or invalids
            ):
                raise RuntimeError(
                    f"DuckDB SCD2 integrity failed for {table}: {source_metrics}."
                )
            metrics[source_name] = source_metrics
    return metrics


def promote(
    *,
    source_candidate: Path,
    candidate: Path,
    production: Path,
    summary_path: Path,
    run_id: str,
    confirmed: bool,
) -> dict[str, Any]:
    if not run_id.isdecimal() or int(run_id) < 1:
        raise ValueError("Source workflow run ID must contain digits only.")
    if not confirmed:
        raise PermissionError(
            "Explicit full-history promotion confirmation is required."
        )
    report, expected = _read_report(summary_path, run_id)
    expected_source = Path(report["candidate"]).resolve()
    if source_candidate.resolve() != expected_source:
        raise ValueError("source candidate path does not match the replay evidence.")
    if candidate.resolve() != Path("data/warehouse/dbt-ci/toronto.duckdb").resolve():
        raise ValueError(
            "dbt candidate must be the isolated data/warehouse/dbt-ci/toronto.duckdb copy."
        )
    if production.resolve() != Path("data/warehouse/toronto.duckdb").resolve():
        raise ValueError("Publication target must be data/warehouse/toronto.duckdb.")
    source_metrics = _validate_database(source_candidate, expected)
    candidate_metrics = _validate_database(candidate, expected)
    if source_metrics != candidate_metrics:
        raise RuntimeError(
            "dbt candidate RAW SCD2 metrics differ from the validated rebuild source."
        )
    source_fingerprint = raw_schema_fingerprint(source_candidate)
    candidate_fingerprint = raw_schema_fingerprint(candidate)
    if source_fingerprint != candidate_fingerprint:
        raise RuntimeError(
            "dbt build changed RAW tables or DLT state; refusing DuckDB publication."
        )

    production.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=production.name + ".mode2-",
        suffix=".tmp",
        dir=production.parent,
        delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        shutil.copy2(candidate, staged)
        if sha256_file(candidate) != sha256_file(staged):
            raise RuntimeError(
                "Staged DuckDB publication copy failed SHA-256 verification."
            )
        os.replace(staged, production)
    finally:
        staged.unlink(missing_ok=True)
    return {
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "run_id": run_id,
        "snapshot_count": int(report["snapshot_count_selected"]),
        "source_fingerprint": source_fingerprint,
        "published_duckdb": str(production.resolve()),
        "source_metrics": source_metrics,
        "production_promotion": "local_validated_copy_ready_for_release_publication",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-candidate", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--production", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--confirm-full-history", action="store_true")
    args = parser.parse_args()
    try:
        result = promote(
            source_candidate=args.source_candidate,
            candidate=args.candidate,
            production=args.production,
            summary_path=args.summary,
            run_id=args.source_run_id,
            confirmed=args.confirm_full_history,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(
            f"DUCKDB MODE 2 PROMOTION FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
