"""Validate an isolated DuckDB replay of the fixed legacy SCD2 baseline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

try:  # CLI execution
    from migrate_legacy_snapshot_artifacts import (
        EXPECTED_LEGACY_RUN_ATTEMPTS,
        EXPECTED_LEGACY_RUN_IDS,
    )
except ImportError:  # module execution
    from scripts.migrate_legacy_snapshot_artifacts import (
        EXPECTED_LEGACY_RUN_ATTEMPTS,
        EXPECTED_LEGACY_RUN_IDS,
    )

SOURCE_TABLES = {
    "active_permits": "raw_active_permits",
    "cleared_permits": "raw_cleared_permits",
}


def _expected_attempt(run_id: str) -> str:
    return str(EXPECTED_LEGACY_RUN_ATTEMPTS.get(run_id, "1"))


BUSINESS_KEY_COLUMNS = ("permit_num", "revision_num", "permit_type")
EXPECTED_METRICS = {
    "active_permits": {"total": 104_321, "current": 98_511, "expired": 5_810},
    "cleared_permits": {"total": 190_323, "current": 190_274, "expired": 49},
}
SCD2_INTEGRITY_METRICS = (
    "null_valid_from",
    "invalid_intervals",
    "duplicate_current_business_keys",
    "overlapping_intervals",
)


def _metrics(connection: duckdb.DuckDBPyConnection, dataset: str, table: str) -> dict:
    table_ref = f'"{dataset}"."{table}"'
    row = connection.execute(
        f"SELECT COUNT(*), "
        "COUNT(*) FILTER (WHERE _dlt_valid_to IS NULL), "
        "COUNT(*) FILTER (WHERE _dlt_valid_to IS NOT NULL), "
        "COUNT(*) FILTER (WHERE _dlt_valid_from IS NULL), "
        "COUNT(*) FILTER (WHERE _dlt_valid_to IS NOT NULL "
        "AND _dlt_valid_to <= _dlt_valid_from) "
        f"FROM {table_ref}"
    ).fetchone()
    total, current, expired, null_from, invalid_intervals = (
        int(value) for value in row
    )
    if total != current + expired:
        raise ValueError(f"{table}: total/current/expired counts do not reconcile.")

    key_sql = ", ".join(BUSINESS_KEY_COLUMNS)
    duplicate_current = int(
        connection.execute(
            f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM {table_ref} "
            "WHERE _dlt_valid_to IS NULL "
            f"GROUP BY {key_sql} HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    )
    # Compare each version with the furthest-reaching earlier interval, not just
    # the immediately previous row. This catches nested overlaps and an earlier
    # open-ended/current interval followed by another version.
    overlaps = int(
        connection.execute(
            "WITH ordered_versions AS ("
            f"SELECT {key_sql}, _dlt_valid_from, "
            "MAX(_dlt_valid_to) OVER ("
            f"PARTITION BY {key_sql} ORDER BY _dlt_valid_from NULLS LAST, _dlt_valid_to NULLS LAST "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) "
            "AS previous_max_valid_to, "
            "BOOL_OR(_dlt_valid_to IS NULL) OVER ("
            f"PARTITION BY {key_sql} ORDER BY _dlt_valid_from NULLS LAST, _dlt_valid_to NULLS LAST "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) "
            f"AS previous_open_interval FROM {table_ref}) "
            "SELECT COUNT(*) FROM ordered_versions "
            "WHERE _dlt_valid_from IS NOT NULL AND ("
            "COALESCE(previous_open_interval, FALSE) "
            "OR previous_max_valid_to > _dlt_valid_from)"
        ).fetchone()[0]
    )
    return {
        "total": total,
        "current": current,
        "expired": expired,
        "null_valid_from": null_from,
        "invalid_intervals": invalid_intervals,
        "duplicate_current_business_keys": duplicate_current,
        "overlapping_intervals": overlaps,
    }


def _validate_source_metrics(source_name: str, metrics: dict) -> None:
    if source_name not in EXPECTED_METRICS:
        raise ValueError(f"No approved historical metrics for source {source_name!r}.")
    expected = EXPECTED_METRICS[source_name]
    actual_counts = {key: metrics[key] for key in ("total", "current", "expired")}
    if actual_counts != expected:
        raise ValueError(
            f"{source_name}: final DuckDB row counts differ from the approved "
            f"historical baseline; expected={expected}, actual={actual_counts}."
        )
    failures = {name: metrics[name] for name in SCD2_INTEGRITY_METRICS if metrics[name]}
    if failures:
        raise ValueError(
            f"{source_name}: DuckDB SCD2 integrity checks failed; "
            f"expected zero for every check, actual={failures}."
        )


def _manifest_records(manifest_root: Path, source_name: str) -> dict:
    source_root = manifest_root / source_name
    if not source_root.is_dir():
        raise FileNotFoundError(source_root)
    paths = sorted(source_root.glob("*_archive.json"))
    if len(paths) != len(EXPECTED_LEGACY_RUN_IDS):
        raise ValueError(
            f"{source_name}: expected {len(EXPECTED_LEGACY_RUN_IDS)} persisted archive manifests, "
            f"found {len(paths)}."
        )

    records = {}
    for path in paths:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        run_id = str(manifest.get("source_run_id", ""))
        attempt = str(manifest.get("source_run_attempt", ""))
        key = (run_id, attempt)
        if key in records:
            raise ValueError(f"{source_name}: duplicate persisted manifest identity {key}.")
        if manifest.get("source_name") != source_name:
            raise ValueError(f"{path}: source name mismatch.")
        if manifest.get("warehouse_status") != "loaded":
            raise ValueError(f"{path}: archive manifest is not marked warehouse-loaded.")
        digest = str(manifest.get("sha256", ""))
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValueError(f"{path}: invalid SHA-256 digest.")
        row_count = int(manifest.get("row_count", 0))
        if row_count < 1:
            raise ValueError(f"{path}: invalid source row count.")
        records[key] = {
            "run_id": run_id,
            "run_attempt": attempt,
            "extracted_at_utc": str(manifest.get("extracted_at_utc", "")),
            "source_rows": row_count,
            "sha256": digest,
        }

    expected = {
        (run_id, _expected_attempt(run_id)) for run_id in EXPECTED_LEGACY_RUN_IDS
    }
    if set(records) != expected:
        raise ValueError(
            f"{source_name}: persisted manifest identities differ from the approved "
            f"28-run baseline; missing={sorted(expected - set(records))}, "
            f"extra={sorted(set(records) - expected)}."
        )
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--manifest-root", type=Path, required=True)
    parser.add_argument("--dataset", default="raw")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if not args.database.is_file():
        raise FileNotFoundError(args.database)
    if not args.dataset.replace("_", "").isalnum():
        raise ValueError("Unsafe DuckDB dataset identifier.")

    with duckdb.connect(str(args.database), read_only=True) as connection:
        connection.execute("SET TimeZone='UTC'")
        ledger = connection.execute(
            "SELECT snapshot_id, source_run_id, source_run_attempt, source_name, status, error "
            "FROM raw.toronto_snapshot_ledger"
        ).fetchall()
        if len(ledger) != len(EXPECTED_LEGACY_RUN_IDS) * len(SOURCE_TABLES):
            raise ValueError(
                f"Expected 56 source ledger entries, found {len(ledger)}."
            )
        expected_ledger = {
            (run_id, _expected_attempt(run_id), source_name)
            for run_id in EXPECTED_LEGACY_RUN_IDS
            for source_name in SOURCE_TABLES
        }
        actual_ledger = set()
        for snapshot_id, run_id, attempt, source_name, status, error in ledger:
            key = (str(run_id), str(attempt), str(source_name))
            if key in actual_ledger:
                raise ValueError(f"Duplicate DuckDB ledger entry: {key}.")
            actual_ledger.add(key)
            if str(snapshot_id) != f"{run_id}:{attempt}":
                raise ValueError(f"Ledger snapshot identity is inconsistent: {key}.")
            if str(status).lower() != "succeeded" or error:
                raise ValueError(f"DuckDB ledger entry is not successful: {key}: {error}")
        if actual_ledger != expected_ledger:
            raise ValueError(
                "DuckDB ledger identities differ from the approved 28-run baseline; "
                f"missing={sorted(expected_ledger - actual_ledger)}, "
                f"extra={sorted(actual_ledger - expected_ledger)}."
            )

        sources = {}
        for source_name, table in SOURCE_TABLES.items():
            records = _manifest_records(args.manifest_root, source_name)
            metrics = _metrics(connection, args.dataset, table)
            _validate_source_metrics(source_name, metrics)
            sources[source_name] = {
                **metrics,
                "snapshots_loaded": len(records),
                "snapshot_loads": [
                    records[(run_id, _expected_attempt(run_id))]
                    for run_id in EXPECTED_LEGACY_RUN_IDS
                ],
            }

    summary = {
        "status": "passed",
        "snapshot_run_ids": list(EXPECTED_LEGACY_RUN_IDS),
        "snapshot_ids": [
            f"{run_id}:{_expected_attempt(run_id)}"
            for run_id in EXPECTED_LEGACY_RUN_IDS
        ],
        "snapshot_count": len(EXPECTED_LEGACY_RUN_IDS),
        "ledger_entries": len(ledger),
        "dataset": args.dataset,
        "sources": sources,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"DUCKDB HISTORICAL REPLAY VALIDATED: {summary['snapshot_count']} snapshots, "
        f"{summary['ledger_entries']} successful source ledger entries."
    )


if __name__ == "__main__":
    main()
