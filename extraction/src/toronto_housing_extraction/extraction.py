"""Orchestration for extracting individual Toronto Open Data sources."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import utils
from .ckan_client import DatastoreUnavailableError, TorontoCKANClient
from .config import CKANConfig, ExtractionConfig, SourceConfig

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceExtraction:
    """Result of one completed source extraction."""

    source_name: str
    extraction_started_at_utc: str
    extraction_completed_at_utc: str
    resource_id: str | None
    resource_name: str | None
    extraction_method: str
    fingerprint: dict[str, Any]
    row_count: int
    quality_warnings: tuple[str, ...]
    columns: tuple[str, ...]
    parquet_path: Path


def _check_required_columns(
    columns: list[str],
    required_columns: tuple[str, ...],
    source_name: str,
) -> None:
    actual = {column.lower() for column in columns}
    missing = [column for column in required_columns if column.lower() not in actual]

    if missing:
        raise ValueError(f"{source_name}: required columns are missing: {missing}")


def _download_fallbacks(
    client: TorontoCKANClient,
    package: dict[str, Any],
    source: SourceConfig,
    output_path: Path,
    *,
    first_resource: dict[str, Any] | None = None,
    expected_rows: int | None = None,
) -> tuple[dict[str, Any], int, dict[str, Any]]:
    """Try CSV/JSON resources until one produces a valid extraction."""
    candidates = client.fallback_resources(package)

    if first_resource is not None:
        candidates = [
            first_resource,
            *[
                resource
                for resource in candidates
                if resource.get("id") != first_resource.get("id")
            ],
        ]

    errors: list[str] = []

    for resource in candidates:
        try:
            row_count = client.download_fallback(
                resource=resource,
                output_path=output_path,
                source_name=source.name,
                required_columns=source.required_columns,
            )

            if expected_rows is not None and row_count != expected_rows:
                raise RuntimeError(
                    f"{source.name}: fallback returned {row_count:,} rows; "
                    f"DataStore reported {expected_rows:,}."
                )

            fingerprint = client.file_fingerprint(
                resource,
                package.get("metadata_modified"),
            )

            return resource, row_count, fingerprint

        except Exception as exc:
            output_path.unlink(missing_ok=True)

            message = (
                f"{resource.get('format')} (resource_id={resource.get('id')!r}): {exc}"
            )
            errors.append(message)

            utils.log_event(
                logging.WARNING,
                "fallback_failed",
                source_name=source.name,
                resource_id=resource.get("id"),
                error=str(exc),
            )

    raise RuntimeError(
        f"{source.name}: all CSV/JSON fallbacks failed: {'; '.join(errors)}"
    )


def extract_source(
    *,
    client: TorontoCKANClient,
    source: SourceConfig,
    ckan: CKANConfig,
    run_dir: Path,
) -> SourceExtraction:
    """Extract one complete source into a Parquet file."""
    started_at = datetime.now(UTC)
    package = client.package_show(source.dataset_slug)
    resource = client.select_resource(package)

    is_datastore = resource.get("datastore_active") is True
    probe: dict[str, Any] | None = None

    if is_datastore:
        try:
            probe = client.probe_datastore(str(resource["id"]))
            _check_required_columns(
                probe["field_names"],
                source.required_columns,
                source.name,
            )
        except DatastoreUnavailableError as exc:
            utils.log_event(
                logging.WARNING,
                "datastore_probe_failed",
                source_name=source.name,
                resource_id=resource.get("id"),
                error=str(exc),
            )
            is_datastore = False

    output_path = run_dir / "parquet" / f"{source.name}.parquet"
    checkpoint_dir = run_dir / "staging" / source.name

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if is_datastore:
        fingerprint = client.source_fingerprint(
            resource,
            probe,
            package.get("metadata_modified"),
        )

        try:
            row_count = client.fetch_datastore(
                resource_id=str(resource["id"]),
                page_size=ckan.page_size,
                checkpoint_dir=checkpoint_dir,
                fingerprint=fingerprint,
                source_name=source.name,
                output_path=output_path,
                pagination_sort=source.pagination_sort,
                required_columns=source.required_columns,
            )
            fingerprint["datastore_total"] = row_count
            extraction_method = "datastore"

        except DatastoreUnavailableError as exc:
            utils.log_event(
                logging.WARNING,
                "datastore_extraction_failed",
                source_name=source.name,
                resource_id=resource.get("id"),
                error=str(exc),
            )

            output_path.unlink(missing_ok=True)
            shutil.rmtree(checkpoint_dir, ignore_errors=True)

            resource, row_count, fingerprint = _download_fallbacks(
                client,
                package,
                source,
                output_path,
                expected_rows=probe["declared_total"],
            )
            extraction_method = "file_fallback"

    else:
        first_fallback = None if resource.get("datastore_active") is True else resource
        resource, row_count, fingerprint = _download_fallbacks(
            client,
            package,
            source,
            output_path,
            first_resource=first_fallback,
        )
        extraction_method = "file_fallback"

    columns = utils.actual_columns(output_path)

    _check_required_columns(
        columns,
        source.required_columns,
        source.name,
    )

    quality_warnings: tuple[str, ...] = ()
    if row_count < source.min_row_count:
        quality_warnings = ("below_minimum_row_count",)

    if utils.parquet_row_count(output_path) != row_count:
        raise RuntimeError(
            f"{source.name}: final Parquet row count does not match extraction."
        )

    completed_at = datetime.now(UTC)

    return SourceExtraction(
        source_name=source.name,
        extraction_started_at_utc=started_at.isoformat(),
        extraction_completed_at_utc=completed_at.isoformat(),
        resource_id=str(resource.get("id")) if resource.get("id") else None,
        resource_name=resource.get("name"),
        extraction_method=extraction_method,
        fingerprint=fingerprint,
        row_count=row_count,
        columns=tuple(columns),
        parquet_path=output_path,
        quality_warnings=quality_warnings,
    )


def extract_sources(
    *,
    client: TorontoCKANClient,
    config: ExtractionConfig,
    run_dir: Path,
) -> tuple[SourceExtraction, ...]:
    """Extract all configured sources as one complete snapshot."""
    results: list[SourceExtraction] = []
    failures: list[str] = []

    total_sources = len(config.sources)

    for index, source in enumerate(config.sources.values(), start=1):
        LOGGER.info(
            "      [%d/%d] Extracting %s...",
            index,
            total_sources,
            source.name,
        )
        try:
            result = extract_source(
                client=client,
                source=source,
                ckan=config.ckan,
                run_dir=run_dir,
            )

            results.append(result)
            LOGGER.info(
                "      [%d/%d] %s completed: %s rows via %s.",
                index,
                total_sources,
                source.name,
                f"{result.row_count:,}",
                result.extraction_method,
            )

        except Exception as exc:
            LOGGER.error(
                "      [%d/%d] %s failed: %s",
                index,
                total_sources,
                source.name,
                exc,
            )
            failures.append(f"{source.name}: {type(exc).__name__}: {exc}")

            utils.log_event(
                logging.ERROR,
                "source_extraction_failed",
                source_name=source.name,
                error=str(exc),
                exception_type=type(exc).__name__,
            )

    if failures:
        raise RuntimeError("Snapshot extraction incomplete: " + "; ".join(failures))

    return tuple(results)
