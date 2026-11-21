"""Load and validate source-level warehouse replay exclusions."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

SOURCE_NAMES = {"active_permits", "cleared_permits"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def load_source_replay_policy(
    path: Path,
) -> dict[tuple[str, str], dict[str, str]]:
    """Load an explicit, versioned source-exclusion policy."""
    try:
        policy = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read source replay policy: {path}") from exc

    if not isinstance(policy, dict):
        raise ValueError("Source replay policy must be a JSON object.")

    if (
        type(policy.get("policy_version")) is not int
        or policy.get("policy_version") != 1
    ):
        raise ValueError("Unsupported source replay policy version.")

    records = policy.get("exclusions")
    if not isinstance(records, list):
        raise ValueError("Policy exclusions must be a list.")

    result: dict[tuple[str, str], dict[str, str]] = {}

    for item in records:
        required = {
            "snapshot_id",
            "source_name",
            "archive_sha256",
            "reason",
        }

        if not isinstance(item, dict) or set(item) != required:
            raise ValueError("Invalid source exclusion record.")

        snapshot_id = item["snapshot_id"]
        source_name = item["source_name"]
        digest = item["archive_sha256"]
        reason = item["reason"]

        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ValueError("Exclusion snapshot ID is invalid.")

        if source_name not in SOURCE_NAMES:
            raise ValueError(f"Unsupported exclusion source: {source_name!r}")

        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise ValueError("Exclusion archive SHA-256 is invalid.")

        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Exclusion reason must be non-empty.")

        key = (snapshot_id, source_name)

        if key in result:
            raise ValueError(f"Duplicate source exclusion: {key}")

        result[key] = {
            "snapshot_id": snapshot_id,
            "source_name": source_name,
            "archive_sha256": digest,
            "reason": reason.strip(),
        }

    return result


def validate_source_replay_policy(
    archives: list[Any],
    exclusions: dict[tuple[str, str], dict[str, str]],
    *,
    require_all: bool = False,
) -> None:
    """Verify exclusion identities against the immutable archive checksums."""
    matched: set[tuple[str, str]] = set()

    for archive in archives:
        for source_name in SOURCE_NAMES:
            key = (archive.snapshot_id, source_name)
            exclusion = exclusions.get(key)

            if exclusion is None:
                continue

            if archive.archive_sha256 != exclusion["archive_sha256"]:
                raise ValueError(
                    "Source exclusion archive checksum mismatch for "
                    f"{archive.snapshot_id}/{source_name}."
                )

            matched.add(key)

    if require_all:
        missing = set(exclusions) - matched

        if missing:
            raise ValueError(
                "Full-history inventory is missing policy exclusions: "
                f"{sorted(missing)}"
            )


def validate_report_source_exclusions(
    report: dict[str, Any],
    exclusions: dict[tuple[str, str], dict[str, str]],
) -> dict[tuple[str, str], str]:
    """Validate summary warehouse statuses against checksum-bound exclusions."""
    snapshots = report.get("snapshots")
    ledger_entries = report.get("ledger_entries")
    if not isinstance(snapshots, list) or not isinstance(ledger_entries, list):
        raise ValueError("Replay summary is missing snapshot or ledger evidence.")

    selected_ids = set()
    for item in snapshots:
        if not isinstance(item, dict):
            raise ValueError("Replay summary contains an invalid snapshot record.")
        snapshot_id = item.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("Replay summary contains an invalid snapshot ID.")
        if snapshot_id in selected_ids:
            raise ValueError(
                f"Replay summary contains duplicate snapshot: {snapshot_id}."
            )
        selected_ids.add(snapshot_id)

    expected_policy_keys = {key for key in exclusions if key[0] in selected_ids}
    if report.get("history_scope") == "full" and expected_policy_keys != set(
        exclusions
    ):
        raise ValueError(
            "Full-history summary does not cover every source policy exclusion: "
            f"{sorted(set(exclusions) - expected_policy_keys)}."
        )

    reported = report.get("source_exclusions")
    if not isinstance(reported, list):
        raise ValueError("Replay summary source_exclusions evidence must be a list.")

    reported_keys: set[tuple[str, str]] = set()
    for item in reported:
        if not isinstance(item, dict):
            raise ValueError("Replay summary contains an invalid source exclusion.")
        key = (item.get("snapshot_id"), item.get("source_name"))
        expected = exclusions.get(key)
        if (
            expected is None
            or key[0] not in selected_ids
            or item.get("archive_sha256") != expected["archive_sha256"]
            or item.get("reason") != expected["reason"]
        ):
            raise ValueError(
                f"Replay summary contains an undeclared or mismatched exclusion: {key}."
            )
        if key in reported_keys:
            raise ValueError(f"Replay summary contains duplicate exclusion: {key}.")
        reported_keys.add(key)

    if reported_keys != expected_policy_keys:
        raise ValueError(
            "Replay summary source exclusions do not exactly match the policy: "
            f"missing={sorted(expected_policy_keys - reported_keys)}, "
            f"unexpected={sorted(reported_keys - expected_policy_keys)}."
        )

    warehouse_statuses: dict[tuple[str, str], str] = {}
    for item in ledger_entries:
        if not isinstance(item, dict):
            raise ValueError("Replay summary ledger entry is invalid.")
        key = (item.get("snapshot_id"), item.get("source_name"))
        if (
            key[0] not in selected_ids
            or key[1] not in SOURCE_NAMES
            or key in warehouse_statuses
        ):
            raise ValueError(
                f"Replay summary has an invalid or duplicate ledger key: {key}."
            )

        expected_status = "excluded" if key in expected_policy_keys else "succeeded"
        observed_status = item.get("warehouse_status")
        if observed_status != expected_status:
            raise ValueError(
                f"Replay summary warehouse status for {key} must be "
                f"{expected_status!r}; observed={observed_status!r}."
            )

        if expected_status == "excluded":
            exclusion = exclusions[key]
            if item.get("archive_sha256") != exclusion["archive_sha256"]:
                raise ValueError(
                    f"Replay summary exclusion checksum differs from policy for {key}."
                )

        warehouse_statuses[key] = expected_status

    wanted_keys = {
        (snapshot_id, source_name)
        for snapshot_id in selected_ids
        for source_name in SOURCE_NAMES
    }
    if set(warehouse_statuses) != wanted_keys:
        raise ValueError(
            "Replay summary native ledger statuses do not cover both sources "
            "for every selected snapshot: "
            f"missing={sorted(wanted_keys - set(warehouse_statuses))}, "
            f"unexpected={sorted(set(warehouse_statuses) - wanted_keys)}."
        )

    return warehouse_statuses
