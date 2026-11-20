"""Validate both replay reports and prove their archives and histories agree."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

SOURCE_NAMES = ("active_permits", "cleared_permits")
BACKENDS = ("duckdb", "snowflake")
INTEGRITY_FIELDS = (
    "invalid_intervals",
    "overlapping_intervals",
    "duplicate_current_business_keys",
    "null_business_keys",
)
IDENTITY_FIELDS = (
    "format_version",
    "order_timestamp",
    "run_id",
    "run_attempt",
    "source_commit",
    "archive_sha256",
    "release_tag",
    "asset_name",
)
QUARANTINE_FIELDS = (
    "quarantined_row_count",
    "quarantined_bad_row_count",
    "quarantined_business_key_count",
    "quarantine_sha256",
)


def _load_json(path: Path, backend: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {backend} replay report {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{backend} replay report must be a JSON object.")
    return value


def _validate_report(
    report: dict[str, Any], backend: str
) -> dict[str, dict[str, dict[str, Any]]]:
    if report.get("backend") != backend or report.get("status") != "passed":
        raise ValueError(
            f"{backend} replay did not pass or report the expected backend."
        )

    streams = report.get("sources")
    if not isinstance(streams, dict) or set(streams) != set(SOURCE_NAMES):
        raise ValueError(f"{backend} report is missing a source stream.")
    for source_name in SOURCE_NAMES:
        stream = streams[source_name]
        if (
            not isinstance(stream, dict)
            or stream.get("status") != "passed"
            or stream.get("failed")
            or stream.get("blocked")
        ):
            raise ValueError(f"{backend}/{source_name} replay stream did not pass.")

    validation = report.get("history_validation")
    if not isinstance(validation, dict) or validation.get("status") != "passed":
        raise ValueError(f"{backend} SCD2 history validation did not pass.")
    validated_sources = validation.get("sources")
    if not isinstance(validated_sources, dict) or set(validated_sources) != set(
        SOURCE_NAMES
    ):
        raise ValueError(f"{backend} history validation is incomplete.")

    for source_name in SOURCE_NAMES:
        result = validated_sources[source_name]
        if not isinstance(result, dict) or result.get("status") != "passed":
            raise ValueError(f"{backend}/{source_name} SCD2 validation did not pass.")
        required_metrics = {
            "total",
            "current",
            "expired",
            "export_rows",
            *INTEGRITY_FIELDS,
        }
        missing = sorted(required_metrics - result.keys())
        if missing:
            raise ValueError(
                f"{backend}/{source_name} validation omitted metrics: {missing}."
            )
        total = int(result["total"])
        current = int(result["current"])
        expired = int(result["expired"])
        if (
            total <= 0
            or current < 0
            or expired < 0
            or total != current + expired
            or int(result["export_rows"]) != total
        ):
            raise ValueError(
                f"{backend}/{source_name} SCD2 counts or history export "
                "do not reconcile."
            )
        violations = {
            field: int(result[field])
            for field in INTEGRITY_FIELDS
            if int(result[field]) != 0
        }
        if violations:
            raise ValueError(
                f"{backend}/{source_name} has SCD2 integrity violations: {violations}."
            )

    records = report.get("ledger_entries")
    if not isinstance(records, list) or not records:
        raise ValueError(f"{backend} report has no persisted archive ledger evidence.")

    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"{backend} ledger entry is invalid.")
        source_name = record.get("source_name")
        snapshot_id = record.get("snapshot_id")
        if (
            source_name not in SOURCE_NAMES
            or not isinstance(snapshot_id, str)
            or not snapshot_id
        ):
            raise ValueError(f"{backend} ledger entry has invalid identity.")
        if source_name in grouped[snapshot_id]:
            raise ValueError(
                f"{backend} ledger has duplicate {snapshot_id}/{source_name} entries."
            )
        if record.get("status") != "succeeded":
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} ledger status is not succeeded."
            )
        if record.get("warehouse_status") != "succeeded":
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} native warehouse "
                "ledger status is not succeeded."
            )

        counts = tuple(record.get(field) for field in QUARANTINE_FIELDS[:3])
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} has invalid quarantine counts."
            )

        quarantined, bad_rows, business_keys = counts
        checksum = record.get("quarantine_sha256")

        if bad_rows > quarantined or business_keys > quarantined:
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} has inconsistent "
                "quarantine counts."
            )

        if quarantined == 0:
            if checksum is not None:
                raise ValueError(
                    f"{backend}/{source_name}/{snapshot_id} has a quarantine "
                    "hash but no quarantined rows."
                )
        elif not isinstance(checksum, str) or not re.fullmatch(
            r"[0-9a-f]{64}", checksum, re.IGNORECASE
        ):
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} has an invalid "
                "quarantine SHA-256."
            )
        missing = [field for field in IDENTITY_FIELDS if field not in record]
        if missing:
            raise ValueError(
                f"{backend}/{source_name}/{snapshot_id} ledger identity "
                f"omits {missing}."
            )
        if record["format_version"] not in (1, 2):
            raise ValueError(
                f"{backend}/{snapshot_id} has an unsupported archive version."
            )
        if not str(record["run_id"]).isdecimal() or int(record["run_id"]) < 1:
            raise ValueError(f"{backend}/{snapshot_id} has an invalid GitHub run ID.")
        if int(record["run_attempt"]) < 1:
            raise ValueError(f"{backend}/{snapshot_id} has an invalid run attempt.")
        if not re.fullmatch(
            r"(?:[0-9a-f]{40}|[0-9a-f]{64})",
            str(record["source_commit"]),
            re.IGNORECASE,
        ):
            raise ValueError(f"{backend}/{snapshot_id} has an invalid source commit.")
        if not re.fullmatch(
            r"[0-9a-f]{64}", str(record["archive_sha256"]), re.IGNORECASE
        ):
            raise ValueError(f"{backend}/{snapshot_id} has an invalid archive SHA-256.")
        try:
            order_timestamp = float(record["order_timestamp"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{backend}/{snapshot_id} has an invalid ordering timestamp."
            ) from exc
        if not math.isfinite(order_timestamp):
            raise ValueError(
                f"{backend}/{snapshot_id} has an invalid ordering timestamp."
            )
        if (
            not str(record["release_tag"]).strip()
            or not str(record["asset_name"]).strip()
        ):
            raise ValueError(
                f"{backend}/{snapshot_id} has incomplete Release identity."
            )
        grouped[snapshot_id][source_name] = record

    for snapshot_id, sources in grouped.items():
        if set(sources) != set(SOURCE_NAMES):
            raise ValueError(
                f"{backend} ledger is incomplete for snapshot {snapshot_id}."
            )
        active = sources["active_permits"]
        cleared = sources["cleared_permits"]
        for field in (
            "format_version",
            "run_id",
            "run_attempt",
            "source_commit",
            "archive_sha256",
            "release_tag",
            "asset_name",
        ):
            if active[field] != cleared[field]:
                raise ValueError(
                    f"{backend} source ledger identity mismatch for "
                    f"{snapshot_id}: {field}."
                )

    return dict(grouped)


def _validate_scope(report: dict[str, Any], backend: str) -> dict[str, Any]:
    """Validate that a report states an internally consistent replay scope."""
    mode = report.get("mode")
    scope = report.get("history_scope")
    if mode not in {"incremental", "rebuild"}:
        raise ValueError(f"{backend} report has no supported replay mode.")
    if report.get("explicit_promotion_required") is not True:
        raise ValueError(
            f"{backend} report must require a separate promotion decision."
        )
    expected_staging = "required" if mode == "incremental" else "not_used"
    if report.get("warehouse_input_staging") != expected_staging:
        raise ValueError(
            f"{backend} {mode} mode has an invalid warehouse input-staging setting."
        )

    selection = report.get("selection")
    if not isinstance(selection, dict):
        raise ValueError(f"{backend} report is missing archive selection evidence.")
    from_date = selection.get("from_date")
    to_date = selection.get("to_date")
    limit = selection.get("limit")
    if from_date is not None and not isinstance(from_date, str):
        raise ValueError(f"{backend} --from-date evidence must be a string or null.")
    if to_date is not None and not isinstance(to_date, str):
        raise ValueError(f"{backend} --to-date evidence must be a string or null.")
    parsed_from = _parse_scope_date(from_date, backend, "--from-date")
    parsed_to = _parse_scope_date(to_date, backend, "--to-date")
    if parsed_from and parsed_to and parsed_from > parsed_to:
        raise ValueError(f"{backend} selection has an inverted date range.")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError(
            f"{backend} last-N evidence must be a positive integer or null."
        )
    if selection.get("limit_semantics") != "most_recent_n_then_replay_chronologically":
        raise ValueError(
            f"{backend} report has unsupported last-N selection semantics."
        )
    if selection.get("ordering") != "extracted_at_utc_ascending":
        raise ValueError(
            f"{backend} archives were not reported in chronological order."
        )
    if selection.get("limit_applied_before_retry_skips") is not True:
        raise ValueError(f"{backend} last-N scope was not fixed before retry skips.")

    has_explicit_selection = bool(from_date or to_date or limit is not None)
    if mode == "incremental":
        if scope != "incremental" or report.get("full_history") is not False:
            raise ValueError(
                f"{backend} incremental mode has inconsistent history scope."
            )
        if has_explicit_selection:
            raise ValueError(
                f"{backend} incremental mode cannot use date or N-snapshot filters."
            )
        expected_basis = "pending_incremental_backlog"
    else:
        if scope not in {"full", "partial"}:
            raise ValueError(f"{backend} rebuild must be labelled full or partial.")
        if report.get("full_history") is not (scope == "full"):
            raise ValueError(
                f"{backend} full_history flag conflicts with its history scope."
            )
        if (scope == "full") == has_explicit_selection:
            raise ValueError(
                f"{backend} rebuild scope does not match its explicit selection."
            )
        expected_basis = (
            "versioned_inventory_verified_v1_v2_release_archives"
            if scope == "full"
            else "explicit_rebuild_selection"
        )

    if report.get("history_scope_basis") != expected_basis:
        raise ValueError(f"{backend} report has an invalid history-scope basis.")
    try:
        discovered = int(report["snapshot_count_discovered"])
        selected = int(report["snapshot_count_selected"])
        pending = int(report["snapshot_count_pending_before_load"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{backend} report is missing archive selection counts."
        ) from exc
    if discovered < 0 or selected < 0 or pending < 0 or pending > selected:
        raise ValueError(f"{backend} report has invalid archive selection counts.")
    if mode == "incremental" and selected != discovered:
        raise ValueError(
            f"{backend} incremental selection does not cover its discovered backlog."
        )
    if mode == "rebuild" and selected > discovered:
        raise ValueError(
            f"{backend} report selected more archives than were discovered."
        )
    if mode == "rebuild" and selected == 0:
        raise ValueError(f"{backend} rebuild selection is empty.")
    if mode == "rebuild" and scope == "full" and selected != discovered:
        raise ValueError(
            f"{backend} full rebuild did not select every discovered archive."
        )

    return {
        "mode": mode,
        "history_scope": scope,
        "full_history": scope == "full",
        "explicit_promotion_required": True,
        "warehouse_input_staging": expected_staging,
        "selection": {
            "from_date": from_date,
            "to_date": to_date,
            "limit": limit,
            "limit_semantics": selection["limit_semantics"],
            "ordering": selection["ordering"],
            "limit_applied_before_retry_skips": True,
        },
        "snapshot_count_discovered": discovered,
        "snapshot_count_selected": selected,
    }


def _parse_scope_date(value: str | None, backend: str, label: str) -> date | None:
    if value is None:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{backend} {label} evidence must use YYYY-MM-DD.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{backend} {label} evidence is not a valid calendar date."
        ) from exc


def validate_backend_report(report: dict[str, Any], backend: str) -> dict[str, Any]:
    """Validate one backend's archive, ledger, quarantine, SCD2 and scope evidence."""
    _validate_report(report, backend)
    return _validate_scope(report, backend)


def validate_reports(
    duckdb_report: dict[str, Any], snowflake_report: dict[str, Any]
) -> dict[str, Any]:
    """Validate each backend and require identical successful archive coverage."""
    checked = {
        "duckdb": _validate_report(duckdb_report, "duckdb"),
        "snowflake": _validate_report(snowflake_report, "snowflake"),
    }
    duck_scope = _validate_scope(duckdb_report, "duckdb")
    snow_scope = _validate_scope(snowflake_report, "snowflake")
    if duck_scope != snow_scope:
        raise ValueError("DuckDB and Snowflake replay modes or history scopes differ.")
    duck_ids = set(checked["duckdb"])
    snow_ids = set(checked["snowflake"])
    if duck_ids != snow_ids:
        raise ValueError(
            "Backend snapshot coverage differs: "
            f"DuckDB-only={sorted(duck_ids - snow_ids)}, "
            f"Snowflake-only={sorted(snow_ids - duck_ids)}."
        )

    for snapshot_id in sorted(duck_ids):
        for source_name in SOURCE_NAMES:
            left = checked["duckdb"][snapshot_id][source_name]
            right = checked["snowflake"][snapshot_id][source_name]
            for field in IDENTITY_FIELDS:
                if left[field] != right[field]:
                    raise ValueError(
                        f"Archive provenance mismatch for {snapshot_id}/"
                        f"{source_name}: {field}."
                    )
            for field in QUARANTINE_FIELDS:
                if left[field] != right[field]:
                    raise ValueError(
                        f"Quarantine evidence mismatch for {snapshot_id}/"
                        f"{source_name}: {field}."
                    )

    snapshots = []
    for snapshot_id in sorted(
        duck_ids,
        key=lambda item: (
            min(
                checked["duckdb"][item][source]["order_timestamp"]
                for source in SOURCE_NAMES
            ),
            item,
        ),
    ):
        active = checked["duckdb"][snapshot_id]["active_permits"]
        snapshots.append(
            {
                "snapshot_id": snapshot_id,
                "format_version": active["format_version"],
                "run_id": active["run_id"],
                "run_attempt": active["run_attempt"],
                "source_commit": active["source_commit"],
                "archive_sha256": active["archive_sha256"],
                "release_tag": active["release_tag"],
                "asset_name": active["asset_name"],
                "quarantine": {
                    source: {
                        field: checked["duckdb"][snapshot_id][source][field]
                        for field in QUARANTINE_FIELDS
                    }
                    for source in SOURCE_NAMES
                },
                "source_order_timestamps": {
                    source: checked["duckdb"][snapshot_id][source]["order_timestamp"]
                    for source in SOURCE_NAMES
                },
            }
        )

    return {
        "status": "passed",
        "snapshot_count": len(duck_ids),
        "source_ledger_entries": len(duck_ids) * len(SOURCE_NAMES),
        "archive_provenance": "passed",
        "backend_snapshot_coverage": "identical",
        "scd2_integrity": "passed",
        "quarantine_parity": "passed",
        "mode": duck_scope["mode"],
        "history_scope": duck_scope["history_scope"],
        "full_history": duck_scope["full_history"],
        "explicit_promotion_required": True,
        "promotion_performed": False,
        "warehouse_input_staging": duck_scope["warehouse_input_staging"],
        "selection": duck_scope["selection"],
        "snapshot_count_discovered": duck_scope["snapshot_count_discovered"],
        "snapshot_count_selected": duck_scope["snapshot_count_selected"],
        "snapshots": snapshots,
        "backends": {
            backend: {
                "candidate": report.get("candidate"),
                "sources": report["history_validation"]["sources"],
            }
            for backend, report in (
                ("duckdb", duckdb_report),
                ("snowflake", snowflake_report),
            )
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-summary", type=Path, required=True)
    parser.add_argument("--snowflake-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    try:
        result = validate_reports(
            _load_json(args.duckdb_summary, "duckdb"),
            _load_json(args.snowflake_summary, "snowflake"),
        )
    except Exception as exc:
        print(f"REPLAY VALIDATION FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
