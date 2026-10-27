"""Build an isolated DuckDB baseline from the exact validated CD snapshot bundle."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import duckdb
from pyarrow import parquet as pq

from toronto_housing_ingestion.config import load_config
from toronto_housing_ingestion.dlt_loader import load_scd2

SOURCE_NAMES = ("active_permits", "cleared_permits")


def _read_json(data: bytes, label: str) -> dict:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object.")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_date(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value!r}")
    return parsed.date().isoformat()


def _extract_and_validate(bundle_path: Path, output_dir: Path, sources: dict) -> dict:
    expected_members = {"bundle.json"}
    for name in SOURCE_NAMES:
        expected_members.update({f"{name}/data.parquet", f"{name}/manifest.json"})

    with ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
        if names != expected_members:
            raise ValueError(
                "Snapshot bundle contents are unexpected. "
                f"Missing={sorted(expected_members - names)}, "
                f"extra={sorted(names - expected_members)}"
            )
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"Snapshot bundle ZIP integrity failed: {bad_member}")

        bundle = _read_json(archive.read("bundle.json"), "bundle.json")
        if bundle.get("format_version") != 1:
            raise ValueError("Unsupported snapshot bundle format version.")
        expected_run_id = os.environ.get("SNAPSHOT_SOURCE_RUN_ID", "")
        expected_commit = os.environ.get("SNAPSHOT_SOURCE_COMMIT", "")
        if not expected_run_id or str(bundle.get("source_run_id")) != expected_run_id:
            raise ValueError("Bundle source run ID differs from the selected CD run.")
        if not expected_commit or bundle.get("source_commit") != expected_commit:
            raise ValueError("Bundle source commit differs from the selected CD run.")

        output_dir.mkdir(parents=True, exist_ok=True)
        for source_name in SOURCE_NAMES:
            source = sources.get(source_name)
            summary = bundle.get("sources", {}).get(source_name)
            if source is None or not isinstance(summary, dict):
                raise ValueError(f"Bundle is missing configured source {source_name}.")

            snapshot_name = f"{source_name}/data.parquet"
            manifest_name = f"{source_name}/manifest.json"
            snapshot_path = output_dir / source_name / "data.parquet"
            manifest_path = output_dir / source_name / "manifest.json"
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            with (
                archive.open(snapshot_name) as source_stream,
                snapshot_path.open("wb") as target_stream,
            ):
                shutil.copyfileobj(source_stream, target_stream)
            manifest_path.write_bytes(archive.read(manifest_name))

            manifest = _read_json(manifest_path.read_bytes(), manifest_name)
            digest = _sha256(snapshot_path)
            if digest != summary.get("sha256") or digest != manifest.get("sha256"):
                raise ValueError(f"{source_name}: Parquet SHA-256 validation failed.")
            parquet = pq.ParquetFile(snapshot_path)
            row_count = int(parquet.metadata.num_rows)
            columns = list(parquet.schema_arrow.names)
            if row_count != summary.get("row_count") or row_count != manifest.get(
                "row_count"
            ):
                raise ValueError(
                    f"{source_name}: row counts disagree with the manifest."
                )
            if sorted(columns) != sorted(summary.get("columns", [])):
                raise ValueError(
                    f"{source_name}: Parquet columns disagree with the manifest."
                )
            lower_columns = {column.lower() for column in columns}
            missing = sorted(
                column
                for column in source.required_columns
                if column.lower() not in lower_columns
            )
            if missing:
                raise ValueError(f"{source_name}: missing required columns: {missing}")
            if (
                manifest.get("status") != "success"
                or manifest.get("warehouse_status") != "loaded"
            ):
                raise ValueError(
                    f"{source_name}: extraction/load manifest is not successful."
                )
            if manifest.get("warehouse_since") != source.warehouse_since:
                raise ValueError(
                    f"{source_name}: warehouse date cutoff differs from configuration."
                )
            if manifest.get("warehouse_date_column") != source.warehouse_date_column:
                raise ValueError(
                    f"{source_name}: warehouse date column differs from configuration."
                )
            print(
                f"Verified {source_name}: {row_count:,} source rows, SHA-256={digest}"
            )

    return bundle


def main() -> None:
    bundle_path = Path(
        os.environ.get(
            "SNAPSHOT_BUNDLE_PATH", "data/snapshot-download/toronto-snapshot-bundle.zip"
        )
    )
    database_path = Path(os.environ["DBT_DUCKDB_PATH"]).resolve()
    if database_path.exists():
        raise FileExistsError(
            f"Refusing to reuse an existing DuckDB parity database: {database_path}"
        )
    database_path.parent.mkdir(parents=True, exist_ok=True)

    config = load_config(Path("open_data_config.yml"))
    if config.warehouse_kind != "duckdb":
        raise ValueError("Parity baseline must use the DuckDB destination.")
    if config.warehouse_credentials != os.environ["DBT_DUCKDB_PATH"]:
        raise ValueError("TORONTO_WAREHOUSE_CREDENTIALS must match DBT_DUCKDB_PATH.")
    sources = {source.name: source for source in config.sources}
    for name in SOURCE_NAMES:
        source = sources.get(name)
        if (
            source is None
            or not source.warehouse_load
            or source.warehouse_strategy != "scd2"
        ):
            raise ValueError(f"{name}: expected an enabled SCD2 source configuration.")

    extracted_dir = Path("data/parity-snapshot")
    bundle = _extract_and_validate(bundle_path, extracted_dir, sources)

    baseline_counts: dict[str, tuple[int, int, int]] = {}
    for name in SOURCE_NAMES:
        result = load_scd2(
            credentials=str(database_path),
            snapshot_path=extracted_dir / name / "data.parquet",
            source=sources[name],
            dataset_name="raw",
            destination_kind="duckdb",
        )
        if result.total_rows is None or result.current_rows is None:
            raise ValueError(f"{name}: DuckDB loader did not return row counts.")
        with duckdb.connect(str(database_path), read_only=True) as connection:
            table = sources[name].warehouse_table
            total, current, expired = connection.execute(
                f"SELECT COUNT(*), COUNT_IF(_dlt_valid_to IS NULL), "
                f'COUNT_IF(_dlt_valid_to IS NOT NULL) FROM raw."{table}"'
            ).fetchone()
        counts = (int(total), int(current or 0), int(expired or 0))
        if counts[0] < 1 or counts[0] != counts[1] + counts[2]:
            raise ValueError(
                f"{name}: fresh DuckDB SCD2 counts do not reconcile: {counts}"
            )
        baseline_counts[name] = counts
        print(
            f"DUCKDB BASELINE {name}: total={counts[0]:,}, current={counts[1]:,}, "
            f"expired={counts[2]:,}",
            flush=True,
        )

    as_of_date = _iso_date(bundle["extraction_started_at_utc"])
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as env:
        env.write(f"TORONTO_DBT_AS_OF_DATE={as_of_date}\n")
    report = {
        "source_run_id": str(bundle["source_run_id"]),
        "source_commit": bundle["source_commit"],
        "as_of_date": as_of_date,
        "bundle_sources": bundle["sources"],
        "duckdb_raw_counts": {
            name: {"total": counts[0], "current": counts[1], "expired": counts[2]}
            for name, counts in baseline_counts.items()
        },
        "duckdb_database": str(database_path),
    }
    Path("duckdb-parity-baseline.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"Prepared fresh DuckDB parity baseline with as_of_date={as_of_date}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
