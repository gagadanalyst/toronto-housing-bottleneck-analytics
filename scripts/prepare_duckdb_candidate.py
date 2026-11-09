"""Copy the last promoted DuckDB database to an isolated run candidate."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

try:
    from load_duckdb_snapshot import _ensure_ledger
except ImportError:
    from scripts.load_duckdb_snapshot import _ensure_ledger


TABLES = {
    "active_permits": "raw_active_permits",
    "cleared_permits": "raw_cleared_permits",
}


def _ensure_quarantine_table(database: Path) -> int:
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS raw.toronto_scd2_quarantine (
                quarantine_id VARCHAR PRIMARY KEY,
                snapshot_id VARCHAR NOT NULL,
                source_name VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                source_run_attempt INTEGER NOT NULL,
                extracted_at_utc TIMESTAMPTZ,
                source_commit VARCHAR,
                release_tag VARCHAR,
                asset_name VARCHAR,
                parquet_sha256 VARCHAR,
                row_ordinal BIGINT NOT NULL,
                business_key_json VARCHAR,
                raw_record_json VARCHAR NOT NULL,
                row_hash VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                detected_at_utc TIMESTAMPTZ NOT NULL,
                UNIQUE(snapshot_id, source_name, row_ordinal, reason)
            )"""
        )
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM raw.toronto_scd2_quarantine"
            ).fetchone()[0]
        )


def _completed_snapshot_ids(database: Path) -> list[str]:
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = connection.execute(
            "SELECT snapshot_id, source_name, status FROM raw.toronto_snapshot_ledger"
        ).fetchall()
    grouped: dict[str, dict[str, str]] = {}
    for snapshot_id, source_name, status in rows:
        grouped.setdefault(str(snapshot_id), {})[str(source_name)] = str(status).lower()
    return sorted(
        snapshot_id
        for snapshot_id, statuses in grouped.items()
        if set(statuses) == {"active_permits", "cleared_permits"}
        and all(
            statuses[name] == "succeeded"
            for name in ("active_permits", "cleared_permits")
        )
    )


def prepare(
    source: Path, candidate: Path, audit_copy: Path
) -> tuple[dict[str, int], list[str]]:
    source = source.resolve()
    candidate = candidate.resolve()
    audit_copy = audit_copy.resolve()
    if source in {candidate, audit_copy} or candidate == audit_copy:
        raise ValueError(
            "Production, candidate and quarantine-audit paths must be distinct."
        )
    if not source.is_file() or source.stat().st_size == 0:
        raise FileNotFoundError(f"Last promoted DuckDB database is missing: {source}")
    candidate.parent.mkdir(parents=True, exist_ok=True)
    temp = candidate.with_name(candidate.name + ".tmp")
    temp.unlink(missing_ok=True)
    audit_copy.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, audit_copy)
    shutil.copy2(source, temp)
    _ensure_ledger(temp)
    _ensure_ledger(audit_copy)
    baseline: dict[str, int] = {}
    baseline_snapshot_ids = _completed_snapshot_ids(source)
    with duckdb.connect(str(temp), read_only=True) as connection:
        for source_name, table in TABLES.items():
            exists = connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='raw' AND table_name=?",
                [table],
            ).fetchone()[0]
            if not exists:
                raise RuntimeError(f"Promoted DuckDB is missing raw.{table}.")
            count = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM raw.{table} WHERE _dlt_valid_to IS NULL"
                ).fetchone()[0]
            )
            if count <= 0:
                raise RuntimeError(
                    f"Promoted DuckDB raw.{table} has no current SCD2 rows."
                )
            baseline[source_name] = count
    os.replace(temp, candidate)
    _ensure_quarantine_table(candidate)
    _ensure_quarantine_table(audit_copy)
    return baseline, baseline_snapshot_ids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument(
        "--audit-copy",
        type=Path,
        default=Path("data/staging/duckdb/quarantine-audit-base.duckdb"),
    )
    parser.add_argument("--github-env", type=Path)
    args = parser.parse_args()
    baseline, baseline_snapshot_ids = prepare(
        args.source, args.candidate, args.audit_copy
    )
    values = {
        "TORONTO_WAREHOUSE_CREDENTIALS": str(args.candidate),
        "TORONTO_CANONICAL_DUCKDB_PATH": str(args.candidate),
        "TORONTO_QUARANTINE_AUDIT_DUCKDB_PATH": str(args.audit_copy),
        "TORONTO_QUARANTINE_BASELINE_COUNTS": json.dumps(baseline, sort_keys=True),
        "TORONTO_QUARANTINE_BASELINE_SNAPSHOT_IDS": json.dumps(
            baseline_snapshot_ids, separators=(",", ":")
        ),
        "DBT_DUCKDB_PATH": str(args.candidate.resolve()),
    }
    if args.github_env:
        with args.github_env.open("a", encoding="utf-8") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    else:
        for key, value in values.items():
            print(f"{key}={value}")
    print(f"Prepared isolated DuckDB candidate: {args.candidate}")
    print(f"Pre-run production current-row baselines: {baseline}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
