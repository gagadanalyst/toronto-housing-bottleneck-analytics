"""Small CKAN client with restartable, bounded-memory extraction."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import utils


class DatastoreUnavailableError(RuntimeError):
    """Raised when DataStore extraction fails and fallback should be attempted."""


class TorontoCKANClient:
    """Reusable client for Toronto's CKAN Action API.

    DataStore extraction is paginated and each page is persisted before its
    checkpoint advances. CSV and JSON resources are supported as fallbacks.
    """

    def __init__(
        self, base_url: str, timeout_seconds: int = 120, retries: int = 4
    ) -> None:
        """Create a pooled HTTP session with retries for transient errors."""
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "Toronto-Housing-Bottleneck-Analytics/0.5.2",
            }
        )
        self.last_quality_warnings: list[dict[str, Any]] = []

    def __enter__(self) -> TorontoCKANClient:
        """Return this client for a context-manager block."""
        return self

    def __exit__(self, *_exc: Any) -> None:
        """Close the HTTP session."""
        self.close()

    def close(self) -> None:
        """Close pooled HTTP connections."""
        self.session.close()

    def action(self, action_name: str, **params: Any) -> dict[str, Any]:
        """Execute one CKAN Action API call and return its result mapping."""
        response = self.session.get(
            f"{self.base_url}/{action_name}",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("success") is not True or not isinstance(
            payload.get("result"), dict
        ):
            raise RuntimeError(
                f"CKAN action '{action_name}' returned an invalid result."
            )
        return payload["result"]

    def package_show(self, dataset_slug: str) -> dict[str, Any]:
        """Return metadata and resources for a CKAN dataset."""
        return self.action("package_show", id=dataset_slug)

    def select_resource(self, package: dict[str, Any]) -> dict[str, Any]:
        """Select DataStore first, then CSV, then JSON."""
        resources = package.get("resources", [])
        for resource in resources:
            if resource.get("datastore_active") is True and resource.get("id"):
                return resource
        for wanted in ("csv", "json"):
            for resource in resources:
                if str(resource.get("format", "")).lower() == wanted and resource.get(
                    "url"
                ):
                    return resource
        raise RuntimeError("No active DataStore, CSV, or JSON resource found.")

    def fallback_resources(self, package: dict[str, Any]) -> list[dict[str, Any]]:
        """Return usable CSV/JSON resources with CSV preferred."""
        resources = [
            resource
            for resource in package.get("resources", [])
            if str(resource.get("format", "")).lower() in {"csv", "json"}
            and resource.get("url")
        ]
        return sorted(
            resources,
            key=lambda resource: str(resource.get("format", "")).lower() != "csv",
        )

    def probe_datastore(self, resource_id: str) -> dict[str, Any]:
        """Read one DataStore row and its schema without materializing the source."""
        try:
            params: dict[str, Any] = {
                "resource_id": resource_id,
                "limit": 1,
                "offset": 0,
            }
            result = self.action("datastore_search", **params)
        except (requests.RequestException, RuntimeError) as exc:
            raise DatastoreUnavailableError(str(exc)) from exc

        records = result.get("records", [])
        fields = result.get("fields", [])
        field_schema = [
            {"id": field.get("id"), "type": field.get("type")} for field in fields
        ]
        schema_hash = sha256(
            json.dumps(field_schema, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return {
            "declared_total": int(result.get("total", len(records))),
            "field_names": [field.get("id") for field in fields],
            "field_schema": field_schema,
            "schema_sha256": schema_hash,
            "sample_record": records[0] if records else None,
        }

    def source_fingerprint(
        self,
        resource: dict[str, Any],
        probe: dict[str, Any],
        package_metadata_modified: str | None,
    ) -> dict[str, Any]:
        """Build source-change metadata for the full published snapshot."""
        return {
            "resource_id": resource.get("id"),
            "resource_hash": resource.get("hash"),
            "resource_size": resource.get("size"),
            "resource_last_modified": resource.get("last_modified"),
            "package_metadata_modified": package_metadata_modified,
            "datastore_total": probe.get("declared_total"),
            "schema_sha256": probe.get("schema_sha256"),
        }

    def file_fingerprint(
        self,
        resource: dict[str, Any],
        package_metadata_modified: str | None,
    ) -> dict[str, Any]:
        """Build metadata used to detect changes in a downloadable resource."""
        url = resource.get("url")
        if not url:
            raise RuntimeError("File resource has no URL.")
        headers: dict[str, Any] = {}
        try:
            response = self.session.head(
                url, timeout=self.timeout_seconds, allow_redirects=True
            )
            response.raise_for_status()
            headers = response.headers
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code not in {403, 405}:
                raise
        return {
            "resource_id": resource.get("id"),
            "resource_hash": resource.get("hash"),
            "resource_size": resource.get("size"),
            "resource_last_modified": resource.get("last_modified"),
            "package_metadata_modified": package_metadata_modified,
            "source_etag": headers.get("ETag"),
            "source_last_modified": headers.get("Last-Modified"),
            "source_size": headers.get("Content-Length"),
        }

    def fetch_datastore(
        self,
        *,
        resource_id: str,
        page_size: int,
        checkpoint_dir: Path,
        fingerprint: dict[str, Any],
        source_name: str,
        output_path: Path,
        pagination_sort: str,
        required_columns: tuple[str, ...],
        date_columns: tuple[str, ...],
    ) -> int:
        """Extract a full DataStore snapshot with restartable pagination."""
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = checkpoint_dir / "checkpoint.json"
        checkpoint = None
        try:
            checkpoint = utils.load_json(checkpoint_path)
        except ValueError:
            utils.log_event(
                logging.WARNING, "checkpoint_corrupt", source_name=source_name
            )
            shutil.rmtree(checkpoint_dir)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

        if checkpoint and not utils.checkpoint_matches(
            checkpoint, resource_id, fingerprint
        ):
            shutil.rmtree(checkpoint_dir)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            checkpoint = None
            utils.log_event(
                logging.INFO,
                "checkpoint_discarded",
                source_name=source_name,
                reason="source_changed",
            )

        parts = sorted(checkpoint_dir.glob("part_*.parquet"))
        offset = 0
        page_number = 0
        declared_total: int | None = None

        if checkpoint:
            offset = int(checkpoint.get("next_offset", 0))
            page_number = int(checkpoint.get("next_page", 0))
            declared_total = int(checkpoint.get("declared_total", 0))
            if checkpoint.get("complete"):
                if not output_path.exists():
                    utils.combine_parquet_parts(parts, output_path)
                return utils.parquet_row_count(output_path)
            parts = [p for p in parts if int(p.stem.split("_")[1]) < page_number]
            utils.log_event(
                logging.INFO,
                "pagination_resume",
                source_name=source_name,
                page=page_number,
                offset=offset,
            )

        self.last_quality_warnings = []
        while True:
            try:
                params: dict[str, Any] = {
                    "resource_id": resource_id,
                    "limit": page_size,
                    "offset": offset,
                    "sort": pagination_sort,
                }
                result = self.action("datastore_search", **params)
            except (requests.RequestException, RuntimeError) as exc:
                raise DatastoreUnavailableError(
                    f"{source_name}: DataStore extraction failed: {exc}"
                ) from exc

            records = result.get("records", [])
            page_total = int(result.get("total", len(records)))
            if declared_total is None:
                declared_total = page_total
            elif page_total != declared_total:
                raise RuntimeError(
                    f"{source_name}: DataStore total changed during extraction "
                    f"({declared_total} -> {page_total})."
                )

            if not records and offset < page_total:
                raise RuntimeError(
                    f"{source_name}: DataStore returned an empty page "
                    "before completion."
                )

            if records:
                frame = pd.DataFrame.from_records(records)
                utils.validate_required_columns(frame, required_columns, source_name)
                utils.validate_data_types(frame, date_columns, source_name)
                part_path = checkpoint_dir / f"part_{page_number:06d}.parquet"
                utils.atomic_write_parquet(frame, part_path)
                parts.append(part_path)

            offset += len(records)
            page_number += 1
            complete = offset >= page_total
            utils.atomic_write_json(
                checkpoint_path,
                {
                    "resource_id": resource_id,
                    "fingerprint": fingerprint,
                    "next_offset": offset,
                    "next_page": page_number,
                    "declared_total": page_total,
                    "complete": complete,
                },
            )
            utils.log_event(
                logging.INFO,
                "page_fetched",
                source_name=source_name,
                page=page_number,
                rows=len(records),
                offset=offset,
                total=page_total,
            )
            if complete:
                break

        utils.combine_parquet_parts(parts, output_path)
        actual = utils.parquet_row_count(output_path)
        if actual != declared_total:
            raise RuntimeError(
                f"{source_name}: Parquet row count mismatch: "
                f"expected {declared_total}, got {actual}."
            )
        shutil.rmtree(checkpoint_dir)
        return actual

    def download_fallback(
        self,
        *,
        resource: dict[str, Any],
        output_path: Path,
        source_name: str,
        required_columns: tuple[str, ...],
        date_columns: tuple[str, ...],
    ) -> int:
        """Download CSV or JSON into Parquet using bounded memory."""
        url = resource.get("url")
        fmt = str(resource.get("format", "")).lower()
        if not url or fmt not in {"csv", "json"}:
            raise ValueError(f"{source_name}: fallback must be CSV or JSON with a URL.")

        with self.session.get(
            url, timeout=self.timeout_seconds, stream=True
        ) as response:
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(suffix=f".{fmt}", delete=False) as temp:
                temp_path = Path(temp.name)
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        temp.write(chunk)

        parts_dir = output_path.parent / f".{output_path.name}.parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        parts: list[Path] = []
        row_count = 0
        try:
            frames = (
                pd.read_csv(
                    temp_path,
                    chunksize=10_000,
                    dtype={"BUILDER_NAME": "string"},
                )
                if fmt == "csv"
                else self._iter_json_frames(temp_path)
            )
            for index, frame in enumerate(frames):
                utils.validate_required_columns(frame, required_columns, source_name)
                utils.validate_data_types(frame, date_columns, source_name)
                if frame.empty:
                    continue
                part_path = parts_dir / f"part_{index:06d}.parquet"
                utils.atomic_write_parquet(frame, part_path)
                parts.append(part_path)
                row_count += len(frame)

            if not parts:
                raise ValueError(f"{source_name}: fallback resource contained no rows.")
            utils.combine_parquet_parts(parts, output_path)
            return utils.parquet_row_count(output_path)
        finally:
            shutil.rmtree(parts_dir, ignore_errors=True)
            temp_path.unlink(missing_ok=True)

    @staticmethod
    def _iter_json_frames(
        path: Path, chunksize: int = 10_000
    ) -> Iterator[pd.DataFrame]:
        """Yield bounded DataFrames from common JSON record-list layouts."""
        import ijson

        for prefix in ("result.records.item", "records.item", "item"):
            batch: list[dict[str, Any]] = []
            found = False
            with path.open("rb") as file:
                for record in ijson.items(file, prefix, use_float=True):
                    if not isinstance(record, dict):
                        raise ValueError("JSON fallback records must be objects.")
                    found = True
                    batch.append(record)
                    if len(batch) >= chunksize:
                        yield pd.DataFrame.from_records(batch)
                        batch = []
                if batch:
                    yield pd.DataFrame.from_records(batch)
            if found:
                return
        raise ValueError("JSON fallback did not contain a supported record list.")
