"""Validate complete full-history evidence before Mode 2 promotion."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.replay_snapshots import discover_archives
from scripts.validate_snapshot_replay import (
    _validate_report,
    _validate_scope,
    validate_reports,
)


def _read(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return value


def _snapshot_identity(item: dict[str, Any]) -> tuple[str, str, str, str]:
    required = ("snapshot_id", "archive_sha256", "release_tag", "asset_name")
    if any(not isinstance(item.get(key), str) or not item[key] for key in required):
        raise ValueError(f"Snapshot evidence omits immutable identity fields: {item!r}")
    return tuple(str(item[key]) for key in required)  # type: ignore[return-value]


def _require_full_report(report: dict[str, Any], backend: str) -> None:
    if report.get("backend") != backend or report.get("status") != "passed":
        raise ValueError(f"{backend} backend report did not pass.")

    if (
        report.get("mode") != "rebuild"
        or report.get("history_scope") != "full"
        or report.get("full_history") is not True
        or report.get("explicit_promotion_required") is not True
        or report.get("warehouse_input_staging") != "not_used"
    ):
        raise ValueError(
            f"{backend} report is not an explicitly promotable full Mode 2 rebuild."
        )

    selection = report.get("selection")
    if not isinstance(selection, dict) or any(
        selection.get(field) is not None
        for field in ("from_date", "to_date", "limit")
    ):
        raise ValueError(
            f"{backend} full rebuild report contains an explicit partial selection."
        )

    discovered = report.get("snapshot_count_discovered")
    selected = report.get("snapshot_count_selected")
    snapshots = report.get("snapshots")
    if (
        type(discovered) is not int
        or type(selected) is not int
        or discovered <= 0
        or selected != discovered
        or not isinstance(snapshots, list)
        or len(snapshots) != selected
    ):
        raise ValueError(f"{backend} does not evidence every discovered archive.")

    identities = [_snapshot_identity(item) for item in snapshots]
    if len(set(identities)) != len(identities):
        raise ValueError(f"{backend} report contains duplicate archive identities.")

    validation = report.get("history_validation")
    if not isinstance(validation, dict) or validation.get("status") != "passed":
        raise ValueError(f"{backend} history validation did not pass.")


def validate_full_rebuild(
    duckdb_report: dict[str, Any],
    snowflake_report: dict[str, Any],
    saved_joint: dict[str, Any],
) -> dict[str, Any]:
    """Retain the existing two-backend validation and parity requirements."""
    _require_full_report(duckdb_report, "duckdb")
    _require_full_report(snowflake_report, "snowflake")

    recomputed = validate_reports(duckdb_report, snowflake_report)

    required = {
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "archive_provenance": "passed",
        "backend_snapshot_coverage": "identical",
        "scd2_integrity": "passed",
        "explicit_promotion_required": True,
        "promotion_performed": False,
    }
    for field, expected in required.items():
        if saved_joint.get(field) != expected:
            raise ValueError(
                f"Saved joint validation field {field!r} must be {expected!r}; "
                f"observed={saved_joint.get(field)!r}."
            )

    for field in (
        "snapshot_count",
        "snapshot_count_discovered",
        "snapshot_count_selected",
    ):
        expected = recomputed.get(field)
        if saved_joint.get(field) != expected:
            raise ValueError(
                f"Saved joint validation {field} differs from independently "
                "recomputed evidence."
            )

    if saved_joint.get("backends") != recomputed.get("backends"):
        raise ValueError(
            "Saved joint backend evidence differs from backend replay reports."
        )

    expected_snapshots = [
        _snapshot_identity(item) for item in recomputed["snapshots"]
    ]
    saved_snapshots = saved_joint.get("snapshots")
    if (
        not isinstance(saved_snapshots, list)
        or [_snapshot_identity(item) for item in saved_snapshots]
        != expected_snapshots
    ):
        raise ValueError(
            "Saved joint archive provenance differs from backend replay reports."
        )

    recomputed["backend_validation_policy"] = "both"
    recomputed["validated_backends"] = ["duckdb", "snowflake"]
    recomputed["cross_backend_parity"] = "passed"
    return recomputed


def validate_duckdb_only(duckdb_report: dict[str, Any]) -> dict[str, Any]:
    """Validate a complete DuckDB rebuild without requiring Snowflake evidence."""
    _require_full_report(duckdb_report, "duckdb")

    # These validators inspect both source ledgers, their archive provenance,
    # snapshot ordering and the detailed SCD2 integrity counters.
    ledgers_by_snapshot = _validate_report(duckdb_report, "duckdb")
    _validate_scope(duckdb_report, "duckdb")

    if not isinstance(ledgers_by_snapshot, dict):
        raise ValueError("DuckDB source-ledger validation returned an invalid result.")

    report_snapshots = duckdb_report["snapshots"]
    summary_ids = set()
    for item in report_snapshots:
        snapshot_id = _snapshot_identity(item)[0]
        if snapshot_id in summary_ids:
            raise ValueError(f"Duplicate DuckDB snapshot ID: {snapshot_id}")
        summary_ids.add(snapshot_id)

    ledger_ids = set(ledgers_by_snapshot)
    if ledger_ids != summary_ids:
        raise ValueError(
            "DuckDB source-ledger snapshot coverage differs from its full-history "
            "summary. "
            f"missing_ledgers={sorted(summary_ids - ledger_ids)}, "
            f"unexpected_ledgers={sorted(ledger_ids - summary_ids)}."
        )

    ledger_entries = 0
    snapshot_evidence: list[dict[str, Any]] = []

    for item in report_snapshots:
        identity = _snapshot_identity(item)
        snapshot_id = identity[0]
        source_records = ledgers_by_snapshot[snapshot_id]

        # _validate_report has already checked the exact Active and Cleared
        # source names, statuses, immutable ledger fields and matching identity.
        if not isinstance(source_records, dict) or len(source_records) != 2:
            raise ValueError(
                f"DuckDB snapshot {snapshot_id} does not have exactly two "
                "validated source-ledger entries."
            )

        for record in source_records.values():
            if not isinstance(record, dict):
                raise ValueError(
                    f"DuckDB snapshot {snapshot_id} has an invalid ledger record."
                )
            if _snapshot_identity(record) != identity:
                raise ValueError(
                    "DuckDB snapshot summary identity differs from its source ledger: "
                    f"{snapshot_id}."
                )

        ledger_entries += len(source_records)

        evidence = dict(item)
        evidence["backends"] = {
            "duckdb": {
                "status": "passed",
                "source_ledgers": sorted(source_records),
            }
        }
        snapshot_evidence.append(evidence)

    selected = duckdb_report["snapshot_count_selected"]
    discovered = duckdb_report["snapshot_count_discovered"]

    if ledger_entries != selected * 2:
        raise ValueError(
            "DuckDB source-ledger entry count is incomplete: "
            f"expected={selected * 2}, observed={ledger_entries}."
        )

    result: dict[str, Any] = {
        "status": "passed",
        "backend_validation_policy": "duckdb-only",
        "validated_backends": ["duckdb"],
        "cross_backend_parity": "not_applicable",
        "snapshot_count": selected,
        "snapshot_count_discovered": discovered,
        "snapshot_count_selected": selected,
        "source_ledger_entries": ledger_entries,
        "archive_provenance": "passed",
        "backend_snapshot_coverage": "not_applicable",
        "scd2_integrity": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "explicit_promotion_required": True,
        "promotion_authorized_by_evidence": False,
        "promotion_performed": False,
        "warehouse_input_staging": "not_used",
        "selection": duckdb_report["selection"],
        "history_validation": duckdb_report["history_validation"],
        "backends": {
            "duckdb": {
                "status": "passed",
                "snapshot_count": selected,
                "source_ledger_entries": ledger_entries,
                "history_validation": duckdb_report["history_validation"],
            }
        },
        "snapshots": snapshot_evidence,
    }

    # Preserve any additional full-history scope evidence supplied by the
    # backend report without inventing or deriving values.
    for field in (
        "history_scope_basis",
        "snapshot_count_pending_before_load",
    ):
        if field in duckdb_report:
            result[field] = duckdb_report[field]

    return result


def verify_current_archives(
    expected: dict[str, Any], *, repository: str, archive_dir: Path
) -> None:
    """Fail closed if an archive was added, removed or replaced after rebuild."""
    with tempfile.TemporaryDirectory(prefix="toronto-rebuild-archive-check-") as temp:
        found = discover_archives(repository, Path(temp), exclude_snapshot_ids=set())

    expected_ids = {
        _snapshot_identity(item) for item in expected.get("snapshots", [])
    }
    current_ids = {
        (item.snapshot_id, item.archive_sha256, item.release_tag, item.asset_name)
        for item in found
    }

    if current_ids != expected_ids:
        raise ValueError(
            "The immutable archive set changed after the rebuild was created; "
            "re-run the full rebuild before promotion. "
            f"added_or_changed={sorted(current_ids - expected_ids)}, "
            f"missing_or_changed={sorted(expected_ids - current_ids)}."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", choices=("both", "duckdb-only"), default="both")
    parser.add_argument("--duckdb-summary", type=Path, required=True)
    parser.add_argument("--snowflake-summary", type=Path)
    parser.add_argument("--joint-validation", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check-current-archives", action="store_true")
    parser.add_argument(
        "--repository",
        default="gagadanalyst/toronto-housing-bottleneck-analytics",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("data/recovery/snapshot-replay/promotion-archive-check"),
    )
    args = parser.parse_args()

    try:
        if not args.check_current_archives:
            raise ValueError(
                "Current immutable archive verification is mandatory for promotion."
            )

        duck = _read(args.duckdb_summary, "DuckDB summary")

        if args.policy == "both":
            if args.snowflake_summary is None or args.joint_validation is None:
                raise ValueError(
                    "Policy 'both' requires --snowflake-summary and "
                    "--joint-validation."
                )
            snow = _read(args.snowflake_summary, "Snowflake summary")
            joint = _read(args.joint_validation, "joint validation")
            result = validate_full_rebuild(duck, snow, joint)
        else:
            if args.snowflake_summary is not None or args.joint_validation is not None:
                raise ValueError(
                    "Policy 'duckdb-only' must not receive Snowflake or joint "
                    "validation evidence."
                )
            result = validate_duckdb_only(duck)

        verify_current_archives(
            result,
            repository=args.repository,
            archive_dir=args.archive_dir,
        )
        result["current_archive_set_verified"] = True
        result["promotion_authorized_by_evidence"] = True
        result["promotion_performed"] = False

        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"FULL REBUILD PROMOTION EVIDENCE PASSED: {args.output}")
        return 0

    except Exception as exc:
        print(
            f"FULL REBUILD PROMOTION BLOCKED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
