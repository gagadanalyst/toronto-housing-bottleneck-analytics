"""Verify DuckDB and Snowflake consumed identical archived source snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

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

EXPECTED_RUN_ID_SET = {str(run_id) for run_id in EXPECTED_LEGACY_RUN_IDS}
EXPECTED_SNAPSHOT_ID_SET = {
    f"{run_id}:{EXPECTED_LEGACY_RUN_ATTEMPTS.get(run_id, '1')}"
    for run_id in EXPECTED_LEGACY_RUN_IDS
}
SOURCE_NAMES = ("active_permits", "cleared_permits")


def _read_summary(path: Path, label: str) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("status") != "passed":
        raise ValueError(f"{label} replay summary is missing or not marked passed: {path}")
    if int(value.get("snapshot_count", 0)) != 28:
        raise ValueError(f"{label} summary does not report exactly 28 snapshots.")
    return value


def _validate_snapshot_scope(summary: dict, label: str) -> None:
    snapshot_ids = summary.get("snapshot_ids")
    if (
        not isinstance(snapshot_ids, list)
        or len(snapshot_ids) != len(EXPECTED_SNAPSHOT_ID_SET)
        or any(not isinstance(value, str) for value in snapshot_ids)
        or len(set(snapshot_ids)) != len(EXPECTED_SNAPSHOT_ID_SET)
        or set(snapshot_ids) != EXPECTED_SNAPSHOT_ID_SET
    ):
        raise ValueError(
            f"{label}: snapshot IDs/attempts do not match the approved 28-run baseline."
        )

    run_ids = summary.get("snapshot_run_ids")
    if (
        not isinstance(run_ids, list)
        or len(run_ids) != len(EXPECTED_RUN_ID_SET)
        or any(not isinstance(value, str) for value in run_ids)
        or len(set(run_ids)) != len(EXPECTED_RUN_ID_SET)
        or set(run_ids) != EXPECTED_RUN_ID_SET
    ):
        raise ValueError(
            f"{label}: run IDs do not match the approved 28-run baseline."
        )


def _snapshot_map(summary: dict, source_name: str, label: str) -> dict:
    source = summary.get("sources", {}).get(source_name)
    if not isinstance(source, dict):
        raise ValueError(f"{label}: missing {source_name} summary.")
    loads = source.get("snapshot_loads")
    if not isinstance(loads, list) or len(loads) != len(EXPECTED_SNAPSHOT_ID_SET):
        raise ValueError(f"{label}: {source_name} must contain 28 snapshot-load records.")
    result = {}
    for item in loads:
        if not isinstance(item, dict):
            raise ValueError(f"{label}: malformed {source_name} snapshot-load record.")
        run_id = str(item.get("run_id", ""))
        attempt = str(item.get("run_attempt", ""))
        key = (run_id, attempt)
        if not run_id.isdigit() or not attempt.isdigit() or key in result:
            raise ValueError(f"{label}: invalid or duplicate {source_name} snapshot identity {key}.")
        digest = str(item.get("sha256", ""))
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValueError(f"{label}: invalid SHA-256 digest for {source_name} {key}.")
        rows = int(item.get("source_rows", 0))
        extracted_at = str(item.get("extracted_at_utc", ""))
        if rows < 1 or not extracted_at:
            raise ValueError(f"{label}: incomplete source metadata for {source_name} {key}.")
        result[key] = (digest, rows, extracted_at)

    expected_keys = {
        tuple(snapshot_id.split(":")) for snapshot_id in EXPECTED_SNAPSHOT_ID_SET
    }
    if set(result) != expected_keys:
        raise ValueError(
            f"{label}: {source_name} source-load identities do not match the "
            "approved 28-run baseline."
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-summary", type=Path, required=True)
    parser.add_argument("--snowflake-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    duckdb_summary = _read_summary(args.duckdb_summary, "DuckDB")
    snowflake_summary = _read_summary(args.snowflake_summary, "Snowflake")
    _validate_snapshot_scope(duckdb_summary, "DuckDB")
    _validate_snapshot_scope(snowflake_summary, "Snowflake")
    duckdb_ids = duckdb_summary["snapshot_ids"]

    sources = {}
    for source_name in SOURCE_NAMES:
        duck_rows = _snapshot_map(duckdb_summary, source_name, "DuckDB")
        snow_rows = _snapshot_map(snowflake_summary, source_name, "Snowflake")
        if duck_rows != snow_rows:
            missing = sorted(set(duck_rows) - set(snow_rows))
            extra = sorted(set(snow_rows) - set(duck_rows))
            changed = sorted(
                key for key in set(duck_rows) & set(snow_rows)
                if duck_rows[key] != snow_rows[key]
            )
            raise ValueError(
                f"{source_name}: the two warehouses did not consume identical archived inputs; "
                f"missing={missing}, extra={extra}, changed={changed}."
            )
        sources[source_name] = {"matched_snapshot_count": len(duck_rows)}
        print(
            f"PASS {source_name}: identical 28 archive identities, attempts, timestamps, "
            "Parquet SHA-256 digests, and source row counts."
        )

    result = {
        "status": "passed",
        "snapshot_count": 28,
        "snapshot_ids": duckdb_ids,
        "sources": sources,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print("PASS: DuckDB and Snowflake consumed exactly the same 56 source archive files.")


if __name__ == "__main__":
    main()
