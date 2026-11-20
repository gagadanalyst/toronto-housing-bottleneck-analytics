"""Extract the latest selected, already-validated archive as publication Parquet assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

from scripts.replay_archive import extract_source, inspect_archive


def extract_latest(
    summary_path: Path,
    archive_dir: Path,
    output_dir: Path,
    *,
    repository: str,
) -> dict[str, str]:
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read replay summary {summary_path}: {exc}") from exc
    snapshots = summary.get("snapshots") if isinstance(summary, dict) else None
    if not isinstance(snapshots, list) or not snapshots:
        raise ValueError("Replay summary contains no selected snapshots to publish.")
    mode = summary.get("mode")
    scope = summary.get("history_scope")
    if mode == "incremental":
        if scope != "incremental":
            raise ValueError(
                "Mode 1 Parquet publication requires incremental history scope."
            )
    elif mode == "rebuild":
        selection = summary.get("selection") or {}
        if (
            scope != "full"
            or summary.get("full_history") is not True
            or summary.get("explicit_promotion_required") is not True
            or summary.get("warehouse_input_staging") != "not_used"
            or any(
                selection.get(key) is not None
                for key in ("from_date", "to_date", "limit")
            )
            or summary.get("snapshot_count_selected")
            != summary.get("snapshot_count_discovered")
        ):
            raise ValueError(
                "Mode 2 Parquet publication requires a validated full-history rebuild."
            )
    else:
        raise ValueError(
            "Parquet publication requires Mode 1 incremental or full Mode 2 evidence."
        )

    latest = snapshots[-1]
    required = ("release_tag", "asset_name", "archive_sha256")
    if not isinstance(latest, dict) or any(not latest.get(key) for key in required):
        raise ValueError(
            "Latest replay snapshot is missing immutable Release identity."
        )
    matches = list(archive_dir.glob(f"archive-*/{latest['asset_name']}"))
    if len(matches) != 1:
        raise ValueError(
            "Could not uniquely locate the latest selected archive in the replay cache: "
            f"{latest['asset_name']} (matches={len(matches)})."
        )

    archive = inspect_archive(
        matches[0],
        str(latest["release_tag"]),
        str(latest["asset_name"]),
        repository=repository,
    )
    if archive.archive_sha256 != latest["archive_sha256"]:
        raise ValueError(
            "Latest selected archive SHA-256 differs from replay evidence."
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="toronto-latest-assets-") as temp:
        temp_path = Path(temp)
        for source_name, output_name in (
            ("active_permits", "toronto-active-permits.parquet"),
            ("cleared_permits", "toronto-cleared-permits.parquet"),
        ):
            extracted = extract_source(archive, source_name, temp_path)
            target = output_dir / output_name
            shutil.copy2(extracted, target)
            source_meta = archive.sources[source_name]
            if (
                not target.is_file()
                or target.stat().st_size != source_meta.parquet_size_bytes
            ):
                raise RuntimeError(
                    f"Published Parquet asset size did not verify: {target}"
                )
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            if digest != source_meta.parquet_sha256:
                raise RuntimeError(
                    f"Published Parquet asset SHA-256 did not verify: {target}"
                )
            results[source_name] = str(target)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data-assets"))
    parser.add_argument(
        "--repository", default="gagadanalyst/toronto-housing-bottleneck-analytics"
    )
    args = parser.parse_args()
    try:
        for source, path in extract_latest(
            args.summary, args.archive_dir, args.output_dir, repository=args.repository
        ).items():
            print(f"Prepared latest {source} publication asset: {path}")
        return 0
    except Exception as exc:
        print(
            f"LATEST SNAPSHOT ASSET EXTRACTION FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
