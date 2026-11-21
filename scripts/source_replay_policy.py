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
