"""Seed quality checks from the previous full-source Release Parquet assets."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pyarrow import parquet as pq

SOURCE_ASSETS = {
    "active_permits": "toronto-active-permits.parquet",
    "cleared_permits": "toronto-cleared-permits.parquet",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def restore_baselines(
    active: Path, cleared: Path, manifest_root: Path
) -> dict[str, int]:
    paths = {"active_permits": active, "cleared_permits": cleared}
    present = {name: path.is_file() for name, path in paths.items()}
    if not any(present.values()):
        print(
            "No previous warehouse-latest Parquet assets found; first-run baseline skipped."
        )
        return {}
    if not all(present.values()):
        raise FileNotFoundError(
            "warehouse-latest must contain both full-source Parquet assets; "
            f"found={present}"
        )

    counts: dict[str, int] = {}
    for name, path in paths.items():
        parquet = pq.ParquetFile(path)
        row_count = int(parquet.metadata.num_rows)
        columns = list(parquet.schema_arrow.names)
        if row_count < 1:
            raise ValueError(f"Previous {name} Parquet is empty: {path}")
        manifest = {
            "source_name": name,
            "status": "success",
            "warehouse_status": "loaded",
            "baseline_kind": "previous_published_full_source_parquet",
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "sha256": sha256(path),
            "raw_path": str(path),
            "extracted_at_utc": "1970-01-01T00:00:00+00:00",
        }
        destination = manifest_root / name / "00000000000000_published_baseline.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        counts[name] = row_count
        print(f"Restored previous {name} baseline: {row_count:,} rows")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active", type=Path, required=True)
    parser.add_argument("--cleared", type=Path, required=True)
    parser.add_argument("--manifest-root", type=Path, default=Path("data/manifests"))
    args = parser.parse_args()
    restore_baselines(args.active, args.cleared, args.manifest_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
