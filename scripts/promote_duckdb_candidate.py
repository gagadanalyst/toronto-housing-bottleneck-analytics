"""Validate and atomically replace the published DuckDB database file."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import duckdb

from toronto_housing_ingestion.quarantine_policy import quarantine_generation_breaches

TABLES = {
    "raw_active_permits": ("permit_num", "revision_num", "permit_type"),
    "raw_cleared_permits": ("permit_num", "revision_num", "permit_type"),
}
SOURCE_TABLES = {
    "active_permits": "raw_active_permits",
    "cleared_permits": "raw_cleared_permits",
}


def _validate_quarantine_reconciliation(
    connection,
    baseline_counts: dict[str, int],
    baseline_snapshot_ids: set[str],
) -> None:
    ledger_rows = connection.execute(
        "SELECT snapshot_id, source_name, quarantined_row_count, "
        "quarantined_bad_row_count, quarantined_business_key_count, "
        "quarantine_sha256, status "
        "FROM raw.toronto_snapshot_ledger"
    ).fetchall()
    quarantine_rows = connection.execute(
        "SELECT quarantine_id, snapshot_id, source_name, row_ordinal, reason, "
        "business_key_json, row_hash, extracted_at_utc, raw_record_json, "
        "parquet_sha256, asset_name "
        "FROM raw.toronto_scd2_quarantine "
        "ORDER BY snapshot_id, source_name, row_ordinal, reason"
    ).fetchall()
    ledger_sources: dict[str, set[str]] = {}
    ledger_pairs: set[tuple[str, str]] = set()
    for snapshot_id, source_name, *_rest in ledger_rows:
        snapshot_key = str(snapshot_id)
        source_key = str(source_name)
        ledger_sources.setdefault(snapshot_key, set()).add(source_key)
        ledger_pairs.add((snapshot_key, source_key))
    for snapshot_id, source_names in ledger_sources.items():
        if source_names != {"active_permits", "cleared_permits"}:
            raise RuntimeError(
                "DuckDB candidate ledger does not contain both sources for "
                f"{snapshot_id}: "
                f"{sorted(source_names)}."
            )

    grouped: dict[tuple[str, str], list[tuple]] = {}
    for (
        quarantine_id,
        snapshot_id,
        source_name,
        ordinal,
        reason,
        business_key_json,
        row_hash,
        extracted_at,
        raw_record_json,
        parquet_sha256,
        asset_name,
    ) in quarantine_rows:
        grouped.setdefault((str(snapshot_id), str(source_name)), []).append(
            (
                str(quarantine_id),
                int(ordinal),
                str(reason),
                business_key_json,
                row_hash,
                extracted_at,
                raw_record_json,
                parquet_sha256,
                asset_name,
            )
        )
    orphaned = set(grouped) - ledger_pairs
    if orphaned:
        raise RuntimeError(
            "DuckDB quarantine contains rows without matching ledger entries: "
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
        _status,
    ) in ledger_rows:
        rows = grouped.get((str(snapshot_id), str(source_name)), [])
        for quarantine_row in rows:
            if not quarantine_row[6] or not quarantine_row[7] or not quarantine_row[8]:
                raise RuntimeError(
                    f"Quarantine row {quarantine_row[0]} is missing its original "
                    "payload or immutable Parquet provenance."
                )
        observed_bad_count = sum(
            1
            for _qid, _ordinal, reason, *_rest in rows
            if reason != "identical_duplicate_record"
        )
        observed_key_count = len(
            {
                row[3]
                for row in rows
                if row[2] == "conflicting_duplicate_business_key" and row[3] is not None
            }
        )
        if (
            int(row_count or 0) != len(rows)
            or int(bad_count or 0) != observed_bad_count
            or int(key_count or 0) != observed_key_count
        ):
            raise RuntimeError(
                f"Quarantine reconciliation failed for {snapshot_id}/{source_name}: "
                "ledger rows/bad rows/conflict keys="
                f"{row_count}/{bad_count}/{key_count}, "
                f"table={len(rows)}/{observed_bad_count}/{observed_key_count}."
            )
        if digest:
            observed = hashlib.sha256(
                "".join(row[0] + "\n" for row in rows).encode("ascii")
            ).hexdigest()
            if observed != str(digest):
                raise RuntimeError(
                    "Quarantine digest does not reconcile for "
                    f"{snapshot_id}/{source_name}."
                )
        table = SOURCE_TABLES.get(str(source_name))
        if table is None:
            raise RuntimeError(f"Unexpected quarantine source name: {source_name!r}.")
        for quarantine_row in rows:
            reason = quarantine_row[2]
            if reason == "identical_duplicate_record":
                continue
            row_hash, extracted_at = quarantine_row[4], quarantine_row[5]
            if row_hash is not None and extracted_at is not None:
                loaded = connection.execute(
                    f"SELECT COUNT(*) FROM raw.{table} "
                    "WHERE row_hash = ? AND _dlt_valid_from = ?",
                    [row_hash, extracted_at],
                ).fetchone()[0]
                if int(loaded or 0):
                    raise RuntimeError(
                        f"Quarantined {reason} row {quarantine_row[0]} appears in "
                        f"clean raw.{table} at its source boundary."
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
    for source_name, (bad_count, baseline) in breaches.items():
        ratio = bad_count / baseline if baseline else float("inf")
        raise RuntimeError(
            f"Quarantine threshold breached for {source_name} in this candidate "
            f"generation: {bad_count}/{baseline} ({ratio:.2%}) >= 25%. "
            f"Contributing snapshots={contributing_snapshots.get(source_name, [])}. "
            "The candidate was not promoted; the audit copy retains the rows."
        )


def validate_candidate(
    candidate: Path, baseline_counts: dict[str, int], baseline_snapshot_ids: set[str]
) -> None:
    with duckdb.connect(str(candidate), read_only=True) as connection:
        names = {
            str(row[0]).lower()
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='raw'"
            ).fetchall()
        }
        required = set(TABLES) | {
            "toronto_snapshot_ledger",
            "toronto_scd2_quarantine",
        }
        missing = required - names
        if missing:
            raise RuntimeError(
                f"DuckDB candidate is missing RAW tables: {sorted(missing)}"
            )

        ledger = connection.execute(
            "SELECT COUNT(*), COUNT_IF(LOWER(status) != 'succeeded') "
            "FROM raw.toronto_snapshot_ledger"
        ).fetchone()
        if not ledger or int(ledger[0] or 0) == 0 or int(ledger[1] or 0) != 0:
            raise RuntimeError(
                "DuckDB candidate contains an empty or incomplete snapshot ledger: "
                f"{ledger}."
            )
        _validate_quarantine_reconciliation(
            connection, baseline_counts, baseline_snapshot_ids
        )

        for table, keys in TABLES.items():
            key_sql = ", ".join(f'"{key}"' for key in keys)
            invalid = connection.execute(
                f"SELECT COUNT(*) FROM raw.{table} "
                "WHERE _dlt_valid_from IS NULL OR "
                "(_dlt_valid_to IS NOT NULL AND _dlt_valid_to < _dlt_valid_from)"
            ).fetchone()[0]
            invalid_keys = connection.execute(
                f"SELECT COUNT(*) FROM raw.{table} WHERE "
                + " OR ".join(
                    f"{key} IS NULL OR TRIM(CAST({key} AS VARCHAR)) = ''"
                    for key in TABLES[table]
                )
            ).fetchone()[0]
            duplicate_current = connection.execute(
                f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM raw.{table} "
                f"WHERE _dlt_valid_to IS NULL GROUP BY {key_sql} HAVING COUNT(*) > 1)"
            ).fetchone()[0]
            overlaps = connection.execute(
                f"WITH ordered AS (SELECT {key_sql}, _dlt_valid_from, _dlt_valid_to, "
                f"LEAD(_dlt_valid_from) OVER (PARTITION BY {key_sql} "
                "ORDER BY _dlt_valid_from) AS next_from "
                f"FROM raw.{table}) SELECT COUNT(*) FROM ordered "
                "WHERE _dlt_valid_to IS NOT NULL AND next_from IS NOT NULL "
                "AND _dlt_valid_to > next_from"
            ).fetchone()[0]
            if (
                int(invalid or 0)
                or int(invalid_keys or 0)
                or int(duplicate_current or 0)
                or int(overlaps or 0)
            ):
                raise RuntimeError(
                    f"DuckDB SCD2 validation failed for {table}: "
                    f"invalid_intervals={invalid}, invalid_keys={invalid_keys}, "
                    f"duplicate_current={duplicate_current}, overlaps={overlaps}."
                )
            print(
                f"Validated raw.{table}: no invalid intervals, overlaps or "
                "duplicate current keys."
            )


def promote(
    candidate: Path,
    production: Path,
    baseline_counts: dict[str, int],
    baseline_snapshot_ids: set[str],
) -> None:
    candidate = candidate.resolve()
    production = production.resolve()
    if candidate == production:
        raise ValueError("Candidate and production paths must be different.")
    if not candidate.is_file() or candidate.stat().st_size == 0:
        raise FileNotFoundError(f"DuckDB candidate is missing: {candidate}")
    validate_candidate(candidate, baseline_counts, baseline_snapshot_ids)
    production.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=production.name + ".promote-",
        suffix=".tmp",
        dir=production.parent,
        delete=False,
    ) as stream:
        staged = Path(stream.name)
    try:
        shutil.copy2(candidate, staged)
        # Validate the closed copy that is about to replace the published file.
        validate_candidate(staged, baseline_counts, baseline_snapshot_ids)
        os.replace(staged, production)
    finally:
        staged.unlink(missing_ok=True)
    print(f"DUCKDB PROMOTION PASSED: {production}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--production", type=Path)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate a staged candidate without replacing the production database.",
    )
    parser.add_argument(
        "--baseline-counts",
        default=os.environ.get("TORONTO_QUARANTINE_BASELINE_COUNTS", "{}"),
    )
    parser.add_argument(
        "--baseline-snapshot-ids",
        default=os.environ.get("TORONTO_QUARANTINE_BASELINE_SNAPSHOT_IDS", "[]"),
    )
    args = parser.parse_args()
    baseline_counts = json.loads(args.baseline_counts)
    baseline_snapshot_ids = set(json.loads(args.baseline_snapshot_ids))
    if args.validate_only:
        validate_candidate(args.candidate, baseline_counts, baseline_snapshot_ids)
        print(f"DUCKDB CANDIDATE VALIDATION PASSED: {args.candidate}")
        return 0
    if args.production is None:
        parser.error("--production is required unless --validate-only is selected.")
    promote(
        args.candidate,
        args.production,
        baseline_counts,
        baseline_snapshot_ids,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
