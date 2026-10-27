"""Package the exact successful source snapshots from the current CD run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime
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


def package_bundle(output: Path) -> None:
    started_at = os.environ.get("SNAPSHOT_EXTRACTION_STARTED_AT", "")
    source_run_id = os.environ.get("GITHUB_RUN_ID", "")
    source_commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
    if not started_at or not source_run_id or not source_commit:
        raise ValueError("Missing extraction marker or GitHub run metadata.")
    started = _timestamp(started_at)

    bundle_sources: dict[str, dict] = {}
    selected: dict[str, tuple[Path, Path]] = {}

    for source_name in SOURCE_NAMES:
        manifest_dir = Path("data/manifests") / source_name
        candidates = []
        for manifest_path in manifest_dir.glob("*.json"):
            try:
                manifest = _read_json(manifest_path)
                extracted_at = manifest.get("extracted_at_utc")
                if extracted_at and _timestamp(extracted_at) >= started:
                    candidates.append((extracted_at, manifest_path, manifest))
            except OSError, ValueError, json.JSONDecodeError:
                continue

        if not candidates:
            raise ValueError(f"No manifest from this run was found for {source_name}.")
        _, manifest_path, manifest = max(candidates, key=lambda item: item[0])

        if (
            manifest.get("status") != "success"
            or manifest.get("warehouse_status") != "loaded"
        ):
            raise ValueError(
                f"{source_name}: latest extraction was not successfully loaded."
            )
        raw_path = Path(str(manifest.get("raw_path", "")))
        if not raw_path.is_file():
            raise ValueError(f"{source_name}: snapshot does not exist: {raw_path}")
        digest = _sha256(raw_path)
        if digest != manifest.get("sha256"):
            raise ValueError(
                f"{source_name}: Parquet SHA-256 does not match its manifest."
            )

        parquet = pq.ParquetFile(raw_path)
        row_count = int(parquet.metadata.num_rows)
        if row_count != int(manifest.get("row_count", -1)) or row_count < 1:
            raise ValueError(
                f"{source_name}: manifest and Parquet row counts disagree."
            )
        columns = list(parquet.schema_arrow.names)
        if sorted(columns) != sorted(manifest.get("columns", [])):
            raise ValueError(f"{source_name}: manifest and Parquet schemas disagree.")

        archive_snapshot = f"{source_name}/data.parquet"
        archive_manifest = f"{source_name}/manifest.json"
        bundle_sources[source_name] = {
            "snapshot": archive_snapshot,
            "manifest": archive_manifest,
            "sha256": digest,
            "row_count": row_count,
            "columns": columns,
            "warehouse_since": manifest.get("warehouse_since"),
            "warehouse_date_column": manifest.get("warehouse_date_column"),
            "resource_id": manifest.get("resource_id"),
            "extracted_at_utc": manifest.get("extracted_at_utc"),
        }
        selected[source_name] = (raw_path, manifest_path)
        print(f"Validated {source_name}: {row_count:,} rows, sha256={digest}")

    bundle = {
        "format_version": 1,
        "source_run_id": source_run_id,
        "source_commit": source_commit,
        "extraction_started_at_utc": started_at,
        "sources": bundle_sources,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_STORED) as archive:
        archive.writestr(
            "bundle.json", json.dumps(bundle, indent=2, sort_keys=True) + "\n"
        )
        for source_name, (snapshot, manifest) in selected.items():
            archive.write(snapshot, f"{source_name}/data.parquet")
            archive.write(manifest, f"{source_name}/manifest.json")

    with ZipFile(output) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(
                f"Snapshot bundle failed ZIP integrity validation: {bad_member}"
            )
    print(
        f"Created validated snapshot bundle: {output} ({output.stat().st_size:,} bytes)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data-assets/toronto-snapshot-bundle.zip"),
    )
    args = parser.parse_args()
    package_bundle(args.output)


if __name__ == "__main__":
    main()
