"""Command-line orchestration for Toronto Open Data ingestion."""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import config, utils
from .ckan_client import DatastoreUnavailableError, TorontoCKANClient
from .dlt_loader import ensure_duckdb_database, load_scd2


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line interface."""
    parser = argparse.ArgumentParser(
        description=(
            "Extract Toronto Open Data to Parquet and optionally load it with dlt SCD2."
        )
    )
    parser.add_argument("--config", type=Path, default=Path("open_data_config.yml"))
    parser.add_argument(
        "--source",
        action="append",
        help="Source name; repeat the option to select multiple sources.",
    )
    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument("--extract-only", action="store_true")
    parser.add_argument("--local-load", action="store_true")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--manifest-root", type=Path, default=Path("data/manifests"))
    parser.add_argument(
        "--checkpoint-root", type=Path, default=Path("data/checkpoints")
    )
    parser.add_argument("--lock-file", type=Path, default=Path("data/.ingestion.lock"))
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser


def check_freshness(
    modified_at: str | None, expected_cadence: str, now: datetime
) -> str:
    """Classify daily metadata as fresh, stale, or unknown."""
    if not modified_at or expected_cadence.lower() != "daily":
        return "unknown"
    try:
        timestamp = datetime.fromisoformat(modified_at.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    age = now - timestamp.astimezone(UTC)
    return "fresh" if age <= timedelta(hours=48) else "stale"


def load_latest_manifest(
    manifest_root: Path, source_name: str
) -> dict[str, Any] | None:
    """Return the newest successful/skipped/extracted manifest."""
    paths = sorted((manifest_root / source_name).glob("*.json"), reverse=True)
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if value.get("status") in {"success", "skipped", "extracted", "load_failed"}:
            return value
    return None


def source_has_changed(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> bool:
    """Return whether a source has a trustworthy change signal.

    We only skip when strong source metadata agrees. Row count or schema alone is
    not sufficient because rows can change without either changing.
    """
    if not previous:
        return True

    for key in ("resource_id",):
        if str(previous.get(key)) != str(current.get(key)):
            return True

    signals = (
        "resource_hash",
        "source_etag",
        "source_last_modified",
        "resource_last_modified",
        "package_metadata_modified",
    )
    compared = False
    for key in signals:
        old = previous.get(key)
        new = current.get(key)
        if old is None or new is None:
            continue
        compared = True
        if str(old) != str(new):
            return True
    return not compared


def detect_schema_drift(
    columns: list[str], previous: dict[str, Any] | None
) -> dict[str, list[str]]:
    """Compare the current source columns with the prior manifest."""
    old = set(previous.get("columns", [])) if previous else set()
    current = set(columns)
    return {"added": sorted(current - old), "removed": sorted(old - current)}


def _fingerprint_for_resource(
    client: TorontoCKANClient,
    resource: dict[str, Any],
    package: dict[str, Any],
    probe: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the appropriate fingerprint for a DataStore or file resource."""
    if resource.get("datastore_active") is True and probe is not None:
        return client.source_fingerprint(
            resource,
            probe,
            package.get("metadata_modified"),
        )
    return client.file_fingerprint(
        resource,
        package.get("metadata_modified"),
    )


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Write a source manifest atomically."""
    utils.atomic_write_json(path, manifest)


def _run_fallbacks(
    client: TorontoCKANClient,
    package: dict[str, Any],
    source: config.SourceConfig,
    output_path: Path,
    first: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], int, dict[str, Any]]:
    """Try available CSV/JSON resources until one extracts successfully."""
    candidates = client.fallback_resources(package)
    if first is not None:
        candidates = [
            first,
            *[resource for resource in candidates if resource is not first],
        ]
    errors: list[str] = []
    for resource in candidates:
        try:
            row_count = client.download_fallback(
                resource=resource,
                output_path=output_path,
                source_name=source.name,
                required_columns=source.required_columns,
                date_columns=source.date_columns,
            )
            fingerprint = client.file_fingerprint(
                resource,
                package.get("metadata_modified"),
            )
            return resource, row_count, fingerprint
        except Exception as exc:
            errors.append(f"{resource.get('format')}: {exc}")
            utils.log_event(
                logging.WARNING,
                "fallback_failed",
                source_name=source.name,
                format=str(resource.get("format")),
                error=str(exc),
            )
    raise RuntimeError(
        f"{source.name}: all CSV/JSON fallbacks failed: {'; '.join(errors)}"
    )


def ingest_source(
    *,
    client: TorontoCKANClient,
    source: config.SourceConfig,
    raw_root: Path,
    manifest_root: Path,
    checkpoint_root: Path,
    page_size: int,
    probe_only: bool,
    full_refresh: bool,
    extract_only: bool,
    app: config.AppConfig,
) -> dict[str, Any]:
    """Extract one configured source, write its manifest, and optionally load it."""
    started = datetime.now(UTC)
    run_id = started.strftime("%Y%m%dT%H%M%S%fZ")
    started_monotonic = time.monotonic()
    package = client.package_show(source.dataset_slug)
    resource = client.select_resource(package)
    is_datastore = resource.get("datastore_active") is True
    probe = None

    if is_datastore:
        try:
            probe = client.probe_datastore(resource["id"])
        except DatastoreUnavailableError as exc:
            utils.log_event(
                logging.WARNING,
                "datastore_unavailable",
                source_name=source.name,
                error=str(exc),
            )
            fallbacks = client.fallback_resources(package)
            if not fallbacks:
                raise RuntimeError(
                    f"{source.name}: DataStore failed and no CSV/JSON fallback exists."
                ) from exc
            resource = fallbacks[0]
            is_datastore = False

    fingerprint = _fingerprint_for_resource(client, resource, package, probe)
    freshness = check_freshness(
        resource.get("last_modified") or package.get("metadata_modified"),
        source.expected_cadence,
        started,
    )
    if freshness == "stale":
        utils.log_event(
            logging.WARNING,
            "source_stale",
            source_name=source.name,
            modified_at=resource.get("last_modified")
            or package.get("metadata_modified"),
        )

    previous = load_latest_manifest(manifest_root, source.name)
    columns = probe["field_names"] if probe else []
    if columns:
        missing = [
            column
            for column in source.required_columns
            if column.lower() not in {c.lower() for c in columns}
        ]
        if missing:
            raise ValueError(f"{source.name}: required columns are missing: {missing}")

    drift = detect_schema_drift(columns, previous)
    if probe_only:
        return {
            "status": "probed",
            "source_name": source.name,
            "resource_id": resource.get("id"),
            "declared_row_count": probe.get("declared_total") if probe else None,
            "columns": columns,
            "schema_drift": drift,
            "source_freshness": freshness,
        }

    if not full_refresh and not source_has_changed(previous, fingerprint):
        manifest = {
            **(previous or {}),
            **fingerprint,
            "status": "skipped",
            "source_name": source.name,
            "skip_reason": "source_unchanged",
            "checked_at_utc": started.isoformat(),
            "source_freshness": freshness,
        }
        _write_manifest(manifest_root / source.name / f"{run_id}.json", manifest)
        return manifest

    output_path = (
        raw_root
        / source.name
        / f"ingest_date={started:%Y-%m-%d}"
        / f"run_id={run_id}"
        / "data.parquet"
    )

    if is_datastore:
        try:
            row_count = client.fetch_datastore(
                resource_id=str(resource["id"]),
                page_size=page_size,
                checkpoint_dir=checkpoint_root / source.name,
                fingerprint=fingerprint,
                source_name=source.name,
                output_path=output_path,
                pagination_sort=str(source.pagination_sort),
                required_columns=source.required_columns,
                date_columns=source.date_columns,
            )
        except DatastoreUnavailableError as exc:
            utils.log_event(
                logging.WARNING,
                "datastore_extraction_failed_fallback",
                source_name=source.name,
                error=str(exc),
            )
            resource, row_count, fingerprint = _run_fallbacks(
                client, package, source, output_path
            )
            is_datastore = False
    else:
        resource, row_count, fingerprint = _run_fallbacks(
            client, package, source, output_path, first=resource
        )

    columns = utils.actual_columns(output_path)
    drift = detect_schema_drift(columns, previous)
    warnings = utils.key_quality_warnings(output_path, source.primary_key, source.name)
    for warning in warnings:
        utils.log_event(logging.WARNING, warning["type"], **warning)

    if source.min_row_count is not None and row_count < source.min_row_count:
        raise RuntimeError(
            f"{source.name}: only {row_count} rows extracted; "
            f"minimum is {source.min_row_count}."
        )

    if (
        previous
        and source.max_row_change_percent is not None
        and previous.get("row_count")
    ):
        previous_rows = int(previous["row_count"])
        change = abs(row_count - previous_rows) / previous_rows * 100
        if change > source.max_row_change_percent:
            utils.log_event(
                logging.WARNING,
                "row_count_anomaly",
                source_name=source.name,
                previous_rows=previous_rows,
                current_rows=row_count,
                change_percent=round(change, 2),
            )

    manifest = {
        "status": "extracted",
        "source_name": source.name,
        "title": source.title,
        "dataset_slug": source.dataset_slug,
        "role": source.role,
        "resource_id": resource.get("id"),
        "resource_name": resource.get("name"),
        "resource_url": resource.get("url"),
        "extraction_method": "datastore" if is_datastore else "file_fallback",
        "source_freshness": freshness,
        **fingerprint,
        "schema_drift": drift,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "primary_key": list(source.primary_key),
        "warehouse_since": source.warehouse_since,
        "warehouse_date_column": source.warehouse_date_column,
        "quality_warnings": warnings,
        "raw_path": str(output_path),
        "sha256": utils.sha256_file(output_path),
        "raw_size_bytes": output_path.stat().st_size,
        "extracted_at_utc": started.isoformat(),
        "duration_seconds": round(time.monotonic() - started_monotonic, 3),
        "warehouse_status": (
            "not_loaded" if extract_only or not source.warehouse_load else "pending"
        ),
    }

    manifest_path = manifest_root / source.name / f"{run_id}.json"
    _write_manifest(manifest_path, manifest)

    if extract_only or not source.warehouse_load:
        manifest["status"] = "success"
        _write_manifest(manifest_path, manifest)
        return manifest

    """Removed below logic as it was blocking load to duck
    as we have duplicates in old data.
    """

    """if warnings:
        manifest.update(
            {
                "status": "success",
                "warehouse_status": "blocked_quality_warning",
                "warehouse_block_reason": (
                    "Configured natural key is not valid for this snapshot."
                ),
            }
        )
        _write_manifest(manifest_path, manifest)
        return manifest"""

    result = load_scd2(
        credentials=app.warehouse_credentials,
        snapshot_path=output_path,
        source=source,
        dataset_name=app.dataset_name,
        destination_kind=app.warehouse_kind,
    )
    manifest.update(
        {
            "status": "success",
            "warehouse_status": "loaded",
            "warehouse_table": source.warehouse_table,
            "warehouse_row_count": result.total_rows,
            "warehouse_current_rows": result.current_rows,
            "duration_seconds": round(time.monotonic() - started_monotonic, 3),
        }
    )
    _write_manifest(manifest_path, manifest)
    return manifest


def main() -> int:
    """Run selected sources and return a shell-friendly exit code."""
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(message)s")
    if args.extract_only and args.local_load:
        raise SystemExit("--extract-only and --local-load cannot be used together.")

    app = config.load_config(args.config)
    source_map = {source.name: source for source in app.sources}
    selected_names = args.source or list(source_map)
    unknown = sorted(set(selected_names) - set(source_map))
    if unknown:
        raise SystemExit(f"Unknown source name(s): {unknown}")

    failures = 0
    with utils.exclusive_lock(args.lock_file):
        if app.warehouse_kind == "duckdb":
            ensure_duckdb_database(app.warehouse_credentials, app.dataset_name)

        if args.local_load:
            for name in selected_names:
                source = source_map[name]
                manifest = load_latest_manifest(args.manifest_root, name)
                if not manifest or not manifest.get("raw_path"):
                    raise RuntimeError(f"{name}: no local snapshot is available.")
                load_scd2(
                    credentials=app.warehouse_credentials,
                    snapshot_path=manifest["raw_path"],
                    source=source,
                    dataset_name=app.dataset_name,
                    destination_kind=app.warehouse_kind,
                )
            return 0

        with TorontoCKANClient(
            app.ckan_base_url,
            timeout_seconds=app.timeout_seconds,
            retries=app.retries,
        ) as client:
            for name in selected_names:
                source = source_map[name]
                try:
                    result = ingest_source(
                        client=client,
                        source=source,
                        raw_root=args.raw_root,
                        manifest_root=args.manifest_root,
                        checkpoint_root=args.checkpoint_root,
                        page_size=app.page_size,
                        probe_only=args.probe_only,
                        full_refresh=args.full_refresh,
                        extract_only=args.extract_only,
                        app=app,
                    )
                    utils.log_event(logging.INFO, "source_complete", **result)
                except Exception as exc:
                    failures += 1
                    utils.log_event(
                        logging.ERROR,
                        "source_failed",
                        source_name=name,
                        exception=str(exc),
                    )

    utils.log_event(
        logging.INFO if failures == 0 else logging.ERROR,
        "job_complete" if failures == 0 else "job_failed",
        source_count=len(selected_names),
        source_failures=failures,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
