"""Source manifests and immutable Parquet snapshot bundles."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zipfile import ZIP_STORED, ZipFile

from pyarrow import parquet as pq

from . import utils
from .config import ExtractionConfig, SourceConfig
from .extraction import SourceExtraction

PACKAGE_NAME = "toronto-housing-extraction"
FULL_COMMIT_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z", re.IGNORECASE)


def build_provenance(
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build producer and execution provenance for a snapshot."""
    env = os.environ if environ is None else environ

    try:
        package_version = distribution_version(PACKAGE_NAME)
    except PackageNotFoundError as exc:
        raise RuntimeError(
            f"{PACKAGE_NAME} must be installed before creating snapshots."
        ) from exc

    if not package_version.strip():
        raise ValueError("Extraction package version is missing.")

    is_github = env.get("GITHUB_ACTIONS", "").lower() == "true"
    source_commit = env.get("SNAPSHOT_SOURCE_COMMIT", "").strip()

    if is_github:
        # workflow_run has its own GITHUB_SHA context. Require the explicitly
        # checked-out upstream commit rather than accidentally recording it.
        if not source_commit:
            if env.get("GITHUB_EVENT_NAME") == "workflow_run":
                raise ValueError(
                    "workflow_run must set SNAPSHOT_SOURCE_COMMIT to "
                    "the checked-out workflow_run.head_sha."
                )
            source_commit = env.get("GITHUB_SHA", "").strip()

        if not FULL_COMMIT_SHA.fullmatch(source_commit):
            raise ValueError(
                "GitHub Actions provenance requires a full source commit SHA."
            )

        required_fields = (
            "GITHUB_REPOSITORY",
            "GITHUB_WORKFLOW",
            "GITHUB_RUN_ID",
            "GITHUB_RUN_ATTEMPT",
            "GITHUB_SERVER_URL",
        )
        values = {name: env.get(name, "").strip() for name in required_fields}
        missing = [name for name, value in values.items() if not value]

        if missing:
            raise ValueError(
                "GitHub Actions provenance is incomplete; missing: "
                + ", ".join(missing)
            )

        repository = values["GITHUB_REPOSITORY"]
        if repository.count("/") != 1 or not all(repository.split("/")):
            raise ValueError("Invalid GITHUB_REPOSITORY value.")

        run_id = values["GITHUB_RUN_ID"]
        attempt_text = values["GITHUB_RUN_ATTEMPT"]

        if not run_id.isdecimal() or int(run_id) < 1:
            raise ValueError("GITHUB_RUN_ID must be a positive integer.")

        if not attempt_text.isdecimal() or int(attempt_text) < 1:
            raise ValueError("GITHUB_RUN_ATTEMPT must be a positive integer.")

        server_url = urlsplit(values["GITHUB_SERVER_URL"])
        if server_url.scheme != "https" or not server_url.netloc:
            raise ValueError("GITHUB_SERVER_URL must be a valid HTTPS URL.")

        run_attempt = int(attempt_text)
        run_url = (
            f"{values['GITHUB_SERVER_URL'].rstrip('/')}/"
            f"{repository}/actions/runs/{run_id}"
        )

        provenance = {
            "schema_version": 1,
            "producer": {
                "package_name": PACKAGE_NAME,
                "package_version": package_version,
                "source_commit": source_commit,
            },
            "execution": {
                "provider": "github_actions",
                "repository": repository,
                "workflow": values["GITHUB_WORKFLOW"],
                "run_id": run_id,
                "run_attempt": run_attempt,
                "run_url": run_url,
            },
        }

        validate_release_provenance(provenance)
        return provenance

    # Local snapshots are valid for development and smoke testing, but are
    # deliberately not eligible for permanent GitHub Release publication.
    if source_commit and not FULL_COMMIT_SHA.fullmatch(source_commit):
        raise ValueError("Local source commit must be a full Git commit SHA.")

    return {
        "schema_version": 1,
        "producer": {
            "package_name": PACKAGE_NAME,
            "package_version": package_version,
            "source_commit": source_commit or None,
        },
        "execution": {
            "provider": "local",
            "repository": None,
            "workflow": None,
            "run_id": None,
            "run_attempt": None,
            "run_url": None,
        },
    }


def validate_release_provenance(provenance: Mapping[str, Any]) -> None:
    """Reject provenance that is unsuitable for permanent archive publication."""
    if not isinstance(provenance, Mapping):
        raise ValueError("Snapshot provenance must be an object.")

    if type(provenance.get("schema_version")) is not int:
        raise ValueError("Unsupported provenance schema version.")

    if provenance["schema_version"] != 1:
        raise ValueError("Unsupported provenance schema version.")

    producer = provenance.get("producer")
    execution = provenance.get("execution")

    if not isinstance(producer, Mapping) or not isinstance(execution, Mapping):
        raise ValueError("Snapshot producer or execution provenance is missing.")

    if producer.get("package_name") != PACKAGE_NAME:
        raise ValueError("Unexpected snapshot producer package.")

    if not str(producer.get("package_version", "")).strip():
        raise ValueError("Snapshot producer package version is missing.")

    if execution.get("provider") != "github_actions":
        raise ValueError("Only GitHub Actions snapshots may be published.")

    source_commit = producer.get("source_commit")
    if not isinstance(source_commit, str) or not FULL_COMMIT_SHA.fullmatch(
        source_commit
    ):
        raise ValueError("A full source commit SHA is required for publication.")

    repository = execution.get("repository")
    workflow = execution.get("workflow")
    run_id = execution.get("run_id")
    run_attempt = execution.get("run_attempt")
    run_url = execution.get("run_url")

    if not isinstance(repository, str) or repository.count("/") != 1:
        raise ValueError("GitHub repository provenance is invalid.")

    if not isinstance(workflow, str) or not workflow.strip():
        raise ValueError("GitHub workflow provenance is missing.")

    if not isinstance(run_id, str) or not run_id.isdecimal():
        raise ValueError("GitHub run ID provenance is invalid.")

    if (
        not isinstance(run_attempt, int)
        or isinstance(run_attempt, bool)
        or run_attempt < 1
    ):
        raise ValueError("GitHub run attempt provenance is invalid.")

    if not isinstance(run_url, str):
        raise ValueError("GitHub run URL provenance is missing.")

    parsed_url = urlsplit(run_url)
    expected_path = f"/{repository}/actions/runs/{run_id}"

    if (
        parsed_url.scheme != "https"
        or not parsed_url.netloc
        or not parsed_url.path.rstrip("/").endswith(expected_path)
    ):
        raise ValueError("GitHub run URL does not match its execution identity.")


def _parse_timestamp(value: str, source_name: str) -> datetime:
    """Parse an extraction timestamp and normalize it to UTC."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{source_name}: invalid extraction timestamp.") from exc

    if parsed.tzinfo is None:
        raise ValueError(
            f"{source_name}: extraction timestamp must include a timezone."
        )

    return parsed.astimezone(UTC)


def build_source_manifest(
    extraction: SourceExtraction,
    source: SourceConfig,
) -> dict[str, Any]:
    """Validate one extracted Parquet file and describe it in a manifest."""
    if extraction.source_name != source.name:
        raise ValueError("Extraction result does not match source configuration.")

    path = extraction.parquet_path

    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"{source.name}: Parquet file is missing or empty.")

    extracted_at = _parse_timestamp(
        extraction.extraction_started_at_utc,
        source.name,
    )
    completed_at = _parse_timestamp(
        extraction.extraction_completed_at_utc,
        source.name,
    )

    if completed_at < extracted_at:
        raise ValueError(
            f"{source.name}: extraction completion precedes extraction start."
        )

    parquet = pq.ParquetFile(path)
    row_count = int(parquet.metadata.num_rows)
    columns = list(parquet.schema_arrow.names)

    # Read the data in batches to verify that the Parquet pages are readable.
    readable_rows = sum(
        batch.num_rows for batch in parquet.iter_batches(batch_size=65_536)
    )

    if readable_rows != row_count:
        raise ValueError(
            f"{source.name}: Parquet data read returned {readable_rows:,} "
            f"rows; metadata declares {row_count:,}."
        )

    if row_count != extraction.row_count:
        raise ValueError(
            f"{source.name}: Parquet contains {row_count:,} rows, "
            f"but extraction reported {extraction.row_count:,}."
        )

    actual_columns = {column.lower() for column in columns}
    missing = [
        column
        for column in source.required_columns
        if column.lower() not in actual_columns
    ]

    if missing:
        raise ValueError(
            f"{source.name}: Parquet is missing required columns: {missing}."
        )

    if set(columns) != set(extraction.columns):
        raise ValueError(
            f"{source.name}: Parquet columns differ from extraction metadata."
        )

    return {
        "manifest_version": 1,
        "source_name": source.name,
        "title": source.title,
        "dataset_slug": source.dataset_slug,
        "portal_url": source.portal_url,
        "resource_id": extraction.resource_id,
        "resource_name": extraction.resource_name,
        "extraction_method": extraction.extraction_method,
        "source_fingerprint": extraction.fingerprint,
        "extraction_started_at_utc": extracted_at.isoformat(),
        "row_count": row_count,
        "columns": columns,
        "parquet_schema": {
            field.name: str(field.type) for field in parquet.schema_arrow
        },
        "required_columns": list(source.required_columns),
        "business_key": list(source.business_key),
        "quality_warnings": list(extraction.quality_warnings),
        "parquet_file": path.name,
        "parquet_size_bytes": path.stat().st_size,
        "parquet_sha256": utils.sha256_file(path),
        "validation_status": "passed",
        "extraction_completed_at_utc": completed_at.isoformat(),
    }


def write_source_manifest(
    manifest: dict[str, Any],
    output_path: Path,
) -> None:
    """Write a source manifest atomically."""
    utils.atomic_write_json(output_path, manifest)


def create_snapshot_bundle(
    extractions: tuple[SourceExtraction, ...],
    config: ExtractionConfig,
    output_path: Path,
    *,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Validate and package all configured sources into one snapshot."""
    results = {item.source_name: item for item in extractions}

    if len(results) != len(extractions):
        raise ValueError("Snapshot contains duplicate source extraction results.")

    expected_sources = set(config.sources)
    actual_sources = set(results)

    if actual_sources != expected_sources:
        missing = sorted(expected_sources - actual_sources)
        unexpected = sorted(actual_sources - expected_sources)
        raise ValueError(
            f"Snapshot source pair is invalid; missing={missing}, "
            f"unexpected={unexpected}."
        )

    manifests: dict[str, dict[str, Any]] = {}

    for source_name, source in config.sources.items():
        manifests[source_name] = build_source_manifest(
            results[source_name],
            source,
        )

    started_at = min(
        _parse_timestamp(item.extraction_started_at_utc, item.source_name)
        for item in extractions
    )

    completed_at = max(
        _parse_timestamp(
            item.extraction_completed_at_utc,
            item.source_name,
        )
        for item in extractions
    )

    if completed_at < started_at:
        raise ValueError("Snapshot completion precedes its extraction start.")

    if snapshot_id is None:
        snapshot_id = uuid.uuid4().hex
    elif not snapshot_id.strip():
        raise ValueError("Snapshot ID must not be empty.")

    sources: dict[str, dict[str, Any]] = {}
    for source_name, manifest in manifests.items():
        sources[source_name] = {
            "data_file": f"{source_name}/data.parquet",
            "manifest_file": f"{source_name}/manifest.json",
            "row_count": manifest["row_count"],
            "columns": manifest["columns"],
            "parquet_size_bytes": manifest["parquet_size_bytes"],
            "parquet_sha256": manifest["parquet_sha256"],
            "quality_warnings": manifest["quality_warnings"],
        }

    provenance = build_provenance()
    bundle = {
        "format_version": 2,
        "snapshot_id": snapshot_id,
        "extraction_started_at_utc": started_at.isoformat(),
        "extraction_completed_at_utc": completed_at.isoformat(),
        "complete": True,
        "validation_status": "passed",
        "provenance": provenance,
        "sources": sources,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        raise FileExistsError(f"Snapshot archive already exists: {output_path}")

    with tempfile.NamedTemporaryFile(
        suffix=".zip",
        dir=output_path.parent,
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)

    try:
        with ZipFile(
            temporary_path,
            "w",
            compression=ZIP_STORED,
        ) as archive:
            archive.writestr(
                "bundle.json",
                json.dumps(bundle, indent=2, sort_keys=True) + "\n",
            )

            for source_name, manifest in manifests.items():
                extraction = results[source_name]
                archive.write(
                    extraction.parquet_path,
                    f"{source_name}/data.parquet",
                )
                archive.writestr(
                    f"{source_name}/manifest.json",
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                )

        # Verify the completed archive before making it the final output.
        with ZipFile(temporary_path, "r") as archive:
            expected_members = {"bundle.json"}

            for source_name in expected_sources:
                expected_members.update(
                    {
                        f"{source_name}/data.parquet",
                        f"{source_name}/manifest.json",
                    }
                )

            if set(archive.namelist()) != expected_members:
                raise ValueError(
                    "Snapshot archive contains unexpected or missing files."
                )

            bad_member = archive.testzip()
            if bad_member is not None:
                raise ValueError(f"Snapshot ZIP integrity failed for {bad_member!r}.")

            archived_bundle = json.loads(archive.read("bundle.json"))

            if archived_bundle != bundle:
                raise ValueError("Archived bundle metadata does not match.")

            for source_name, manifest in manifests.items():
                data_name = f"{source_name}/data.parquet"
                manifest_name = f"{source_name}/manifest.json"

                archived_manifest = json.loads(archive.read(manifest_name))
                if archived_manifest != manifest:
                    raise ValueError(
                        f"{source_name}: archived manifest does not match."
                    )

                digest = hashlib.sha256()
                with archive.open(data_name) as data_file:
                    for chunk in iter(
                        lambda: data_file.read(1024 * 1024),
                        b"",
                    ):
                        digest.update(chunk)

                if digest.hexdigest() != manifest["parquet_sha256"]:
                    raise ValueError(
                        f"{source_name}: archived Parquet checksum mismatch."
                    )

        os.link(temporary_path, output_path)

    finally:
        temporary_path.unlink(missing_ok=True)

    return bundle
