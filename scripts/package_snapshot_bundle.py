"""Package the exact successful source snapshots from the current CD run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

from pyarrow import parquet as pq

SOURCE_NAMES = ("active_permits", "cleared_permits")


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value!r}")
    return parsed


def package_bundle(
    output: Path,
    *,
    stage: str = "warehouse_validated",
    allow_partial: bool = False,
) -> None:
    if stage not in {"raw", "warehouse_validated"}:
        raise ValueError(f"Unsupported snapshot stage: {stage!r}")
    if allow_partial and stage != "raw":
        raise ValueError("--allow-partial is supported only for raw snapshots.")

    started_at = os.environ.get("SNAPSHOT_EXTRACTION_STARTED_AT", "")
    source_run_id = os.environ.get("GITHUB_RUN_ID", "")
    source_commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
    source_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    if not started_at or not source_run_id or not source_commit:
        raise ValueError("Missing extraction marker or GitHub run metadata.")
    if not source_run_id.isdigit() or not source_attempt.isdigit():
        raise ValueError("GitHub run ID and attempt must contain digits only.")

    started = _timestamp(started_at)
    started_epoch = started.timestamp()
    bundle_sources: dict[str, dict] = {}
    selected: dict[str, tuple[Path | None, bytes]] = {}
    source_failures: dict[str, list[str]] = {}
    missing_sources: list[str] = []

    for source_name in SOURCE_NAMES:
        failures: list[str] = []
        manifest_dir = Path("data/manifests") / source_name
        candidates: list[tuple[datetime, Path, dict]] = []

        if manifest_dir.exists():
            for manifest_path in manifest_dir.glob("*.json"):
                try:
                    manifest = _read_json(manifest_path)
                    extracted_at = manifest.get("extracted_at_utc")
                    if extracted_at and _timestamp(extracted_at) >= started:
                        candidates.append(
                            (_timestamp(extracted_at), manifest_path, manifest)
                        )
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    try:
                        is_current_file = manifest_path.stat().st_mtime >= started_epoch
                    except OSError:
                        is_current_file = False
                    if is_current_file:
                        failures.append(
                            f"manifest {manifest_path.name}: {type(exc).__name__}: {exc}"
                        )

        manifest_path: Path | None = None
        manifest: dict = {}
        raw_path: Path | None = None
        manifest_bytes: bytes

        if candidates:
            if stage == "warehouse_validated":
                candidates = [
                    item
                    for item in candidates
                    if item[2].get("status") == "success"
                    and item[2].get("warehouse_status") == "loaded"
                    and item[2].get("raw_path")
                    and Path(str(item[2]["raw_path"])).is_file()
                ]
            else:
                candidates = [
                    item
                    for item in candidates
                    if item[2].get("raw_path")
                    and Path(str(item[2]["raw_path"])).is_file()
                ]
            if not candidates:
                raise ValueError(
                    f"No usable local Parquet manifest was found for {source_name}."
                )
            _, manifest_path, manifest = max(candidates, key=lambda item: item[0])
            raw_value = manifest.get("raw_path")
            raw_path = Path(str(raw_value)) if raw_value else None
            try:
                manifest_bytes = manifest_path.read_bytes()
            except OSError as exc:
                failures.append(f"cannot read manifest: {exc}")
                manifest_bytes = json.dumps(
                    {"source_name": source_name, "archive_error": str(exc)},
                    sort_keys=True,
                ).encode()
        else:
            # If extraction created Parquet but failed before its manifest was
            # written, retain the newest file modified after this run began.
            raw_root = Path("data/raw") / source_name
            recent = (
                sorted(
                    (
                        p
                        for p in raw_root.rglob("*.parquet")
                        if p.is_file() and p.stat().st_mtime >= started_epoch
                    ),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if raw_root.exists()
                else []
            )
            if recent:
                raw_path = recent[0]
                failures.append("no valid extraction manifest was found for this run")
                manifest = {
                    "source_name": source_name,
                    "status": "extracted_unmanifested",
                    "warehouse_status": "blocked",
                    "raw_path": str(raw_path),
                    "extracted_at_utc": datetime.fromtimestamp(
                        raw_path.stat().st_mtime, UTC
                    ).isoformat(),
                    "archive_error": "manifest missing or invalid",
                }
                manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode()
            else:
                missing_sources.append(source_name)
                failures.append(
                    "no Parquet snapshot or usable manifest was found for this run"
                )
                source_failures[source_name] = failures
                continue

        has_snapshot = raw_path is not None and raw_path.is_file()
        if not has_snapshot:
            failures.append(f"raw Parquet file is missing: {raw_path}")

        actual_digest = None
        row_count = None
        columns: list[str] = []
        if has_snapshot:
            actual_digest = _sha256(raw_path)
            if actual_digest != manifest.get("sha256"):
                failures.append(
                    "Parquet SHA-256 does not match its extraction manifest"
                )
            try:
                parquet = pq.ParquetFile(raw_path)
                row_count = int(parquet.metadata.num_rows)
                columns = list(parquet.schema_arrow.names)
                if row_count != int(manifest.get("row_count", -1)):
                    failures.append(
                        "Parquet row count does not match its extraction manifest"
                    )
                if sorted(columns) != sorted(manifest.get("columns", [])):
                    failures.append(
                        "Parquet schema does not match its extraction manifest"
                    )
            except Exception as exc:
                failures.append(
                    f"Parquet validation failed: {type(exc).__name__}: {exc}"
                )

        required_columns = manifest.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
            failures.append("manifest required_columns is not a list")
        available_columns = {column.lower() for column in columns}
        missing_required = sorted(
            str(column)
            for column in required_columns
            if str(column).lower() not in available_columns
        )
        blockers = list(manifest.get("warehouse_quality_blockers", []))
        if not required_columns:
            blockers.append("required_columns_not_recorded")
        if missing_required:
            blockers.append("missing_required_columns")
        if row_count is None or row_count < 1:
            blockers.append("empty_or_unreadable_snapshot")
        if manifest.get("warehouse_eligible") is not True:
            blockers.extend(
                blocker
                for blocker in manifest.get("warehouse_quality_blockers", [])
                if blocker not in blockers
            )

        expected_statuses = (
            {"success"}
            if stage == "warehouse_validated"
            else {"success", "extracted", "load_failed"}
        )
        expected_warehouse = (
            {"loaded"}
            if stage == "warehouse_validated"
            else {"not_loaded", "pending", "failed", "blocked"}
        )
        if manifest.get("status") not in expected_statuses:
            blockers.append(f"unexpected_manifest_status:{manifest.get('status')}")
        if manifest.get("warehouse_status") not in expected_warehouse:
            blockers.append(
                f"unexpected_warehouse_status:{manifest.get('warehouse_status')}"
            )

        # A source with valid bytes but quality blockers is retained, not loaded.
        eligible = (
            has_snapshot
            and not failures
            and not blockers
            and manifest.get("warehouse_eligible") is True
        )
        source_summary = {
            "snapshot": f"{source_name}/data.parquet" if has_snapshot else None,
            "manifest": f"{source_name}/manifest.json",
            "sha256": actual_digest,
            "row_count": row_count,
            "columns": columns,
            "required_columns": required_columns,
            "warehouse_since": manifest.get("warehouse_since"),
            "warehouse_date_column": manifest.get("warehouse_date_column"),
            "resource_id": manifest.get("resource_id"),
            "extracted_at_utc": manifest.get("extracted_at_utc"),
            "warehouse_eligible": eligible,
            "warehouse_quality_blockers": sorted(set(blockers)),
            "validation_errors": failures,
            "has_snapshot": has_snapshot,
        }
        bundle_sources[source_name] = source_summary
        selected[source_name] = (raw_path if has_snapshot else None, manifest_bytes)
        if failures:
            source_failures[source_name] = failures
        print(
            f"Retained {source_name}: snapshot={has_snapshot}, rows={row_count}, "
            f"warehouse_eligible={eligible}, errors={len(failures)}"
        )

    if not selected:
        raise ValueError(
            "No source Parquet was available to archive. Failure details: "
            + json.dumps(source_failures, sort_keys=True)
        )

    complete = (
        set(selected) == set(SOURCE_NAMES)
        and all(bundle_sources[name]["has_snapshot"] for name in SOURCE_NAMES)
        and not source_failures
    )
    approved = complete and all(
        bundle_sources[name]["warehouse_eligible"] for name in SOURCE_NAMES
    )
    if stage == "warehouse_validated" and not approved:
        raise ValueError(
            "Warehouse-validated bundle requires both valid, approved sources: "
            + json.dumps(source_failures, sort_keys=True)
        )
    if stage == "raw" and not complete and not allow_partial:
        raise ValueError("A complete raw bundle was required.")

    bundle = {
        "format_version": 1,
        "snapshot_stage": stage,
        "complete": complete,
        "approved_for_warehouse": approved,
        "missing_sources": missing_sources,
        "source_failures": source_failures,
        "source_run_id": source_run_id,
        "source_run_attempt": source_attempt,
        "source_commit": source_commit,
        "extraction_started_at_utc": started.isoformat(),
        "sources": bundle_sources,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_STORED) as archive:
        archive.writestr(
            "bundle.json", json.dumps(bundle, indent=2, sort_keys=True) + "\n"
        )
        for source_name, (snapshot, manifest_bytes) in selected.items():
            if snapshot is not None:
                archive.write(snapshot, f"{source_name}/data.parquet")
            archive.writestr(f"{source_name}/manifest.json", manifest_bytes)

    with ZipFile(output) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"Snapshot bundle failed ZIP integrity: {bad_member}")
    print(
        f"Created {stage} snapshot bundle: {output} ({output.stat().st_size:,} bytes); "
        f"complete={complete}; approved_for_warehouse={approved}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data-assets/toronto-snapshot-bundle.zip"),
    )
    parser.add_argument(
        "--stage",
        choices=("raw", "warehouse_validated"),
        default="warehouse_validated",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Preserve whichever source extracts succeeded.",
    )
    args = parser.parse_args()

    package_bundle(
        args.output,
        stage=args.stage,
        allow_partial=args.allow_partial,
    )


if __name__ == "__main__":
    main()
