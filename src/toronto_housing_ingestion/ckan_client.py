"""Small CKAN client with restartable, bounded-memory extraction."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
from collections.abc import Iterator
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import utils

_SOURCE_FIELD_TYPES: dict[str, str] = {
    "_id": "int4",
    "PERMIT_NUM": "text",
    "REVISION_NUM": "text",
    "PERMIT_TYPE": "text",
    "STRUCTURE_TYPE": "text",
    "WORK": "text",
    "STREET_NUM": "text",
    "STREET_NAME": "text",
    "STREET_TYPE": "text",
    "STREET_DIRECTION": "text",
    "POSTAL": "text",
    "GEO_ID": "text",
    "WARD_GRID": "text",
    "APPLICATION_DATE": "date",
    "ISSUED_DATE": "date",
    "COMPLETED_DATE": "date",
    "STATUS": "text",
    "DESCRIPTION": "text",
    "CURRENT_USE": "text",
    "PROPOSED_USE": "text",
    "DWELLING_UNITS_CREATED": "text",
    "DWELLING_UNITS_LOST": "text",
    "EST_CONST_COST": "text",
    "ASSEMBLY": "float8",
    "INSTITUTIONAL": "float8",
    "RESIDENTIAL": "float8",
    "BUSINESS_AND_PERSONAL_SERVICES": "float8",
    "MERCANTILE": "float8",
    "INDUSTRIAL": "float8",
    "INTERIOR_ALTERATIONS": "float8",
    "DEMOLITION": "float8",
    "BUILDER_NAME": "text",
}
_SOURCE_TEXT_COLUMNS = frozenset(
    name
    for name, field_type in _SOURCE_FIELD_TYPES.items()
    if field_type in {"text", "date"}
)
_SOURCE_FLOAT_COLUMNS = frozenset(
    name for name, field_type in _SOURCE_FIELD_TYPES.items() if field_type == "float8"
)
_SOURCE_INTEGER_COLUMNS = frozenset(
    name
    for name, field_type in _SOURCE_FIELD_TYPES.items()
    if field_type in {"int2", "int4", "int8"}
)


def _source_csv_dtypes() -> dict[str, str]:
    # Prevent inference for source text and date fields.
    return {column: "string" for column in _SOURCE_TEXT_COLUMNS}


def _source_text_value(value: object, column: str, declared_type: str) -> object:
    # Preserve original text. Reject numbers because padding may already be lost.
    if value is None or value is pd.NA:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except TypeError, ValueError:
        pass
    if isinstance(value, str):
        return pd.NA if value == "" else value
    value_kind = type(value).__name__
    raise ValueError(
        f"CKAN field {column!r} is declared {declared_type} but received "
        f"a non-string value ({value_kind}: {value!r}); refusing lossy coercion. "
        "A numeric JSON value may already have lost leading zeroes."
    )


def _normalize_source_schema(frame: pd.DataFrame) -> pd.DataFrame:
    # Stabilize all known fields across DataStore, CSV and JSON routes.
    # Raw date columns stay strings for legacy hash compatibility.
    # Unknown fields are preserved after the canonical fields for drift review.
    actual_columns = {str(column).upper(): column for column in frame.columns}
    for wanted, declared_type in _SOURCE_FIELD_TYPES.items():
        column = actual_columns.get(wanted.upper())
        if column is None:
            continue
        if declared_type in {"text", "date"}:
            values = [
                _source_text_value(value, str(column), declared_type)
                for value in frame[column].tolist()
            ]
            frame[column] = pd.Series(values, index=frame.index, dtype="string")
        elif declared_type == "float8":
            series = frame[column].replace("", pd.NA)
            try:
                converted = pd.to_numeric(series, errors="raise")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"CKAN field {column!r} is declared float8 but contains non-numeric values."
                ) from exc
            frame[column] = converted.astype("float64")
        elif declared_type in {"int2", "int4", "int8"}:
            series = frame[column].replace("", pd.NA)
            try:
                converted = pd.to_numeric(series, errors="raise")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"CKAN field {column!r} is declared {declared_type} but contains non-numeric values."
                ) from exc
            non_null = converted.dropna()
            if not (non_null % 1 == 0).all():
                raise ValueError(
                    f"CKAN field {column!r} is declared {declared_type} but contains fractional values."
                )
            integer_dtype = {"int2": "Int16", "int4": "Int32", "int8": "Int64"}[
                declared_type
            ]
            frame[column] = converted.astype(integer_dtype)

    # Input resource order may differ even when the source schema is identical.
    actual_columns = {str(column).upper(): column for column in frame.columns}
    canonical = [
        actual_columns[name.upper()]
        for name in _SOURCE_FIELD_TYPES
        if name.upper() in actual_columns
    ]
    known_names = {name.upper() for name in _SOURCE_FIELD_TYPES}
    unknown = [
        column for column in frame.columns if str(column).upper() not in known_names
    ]
    return frame.loc[:, canonical + unknown]


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
            if resource.get("datastore_active") is not True
            and str(resource.get("format", "")).lower() in {"csv", "json"}
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
        max_snapshot_restarts = 2
        snapshot_restarts = 0

        if checkpoint:
            offset = int(checkpoint.get("next_offset", 0))
            page_number = int(checkpoint.get("next_page", 0))
            declared_total = int(checkpoint.get("declared_total", 0))

            if checkpoint.get("complete"):
                actual_rows = None
                try:
                    if not output_path.exists():
                        utils.combine_parquet_parts(parts, output_path)

                    actual_rows = utils.parquet_row_count(output_path)
                    if actual_rows != declared_total:
                        raise RuntimeError(
                            f"{source_name}: Parquet row count mismatch: "
                            f"expected {declared_total}, got {actual_rows}."
                        )
                except Exception as exc:
                    utils.log_event(
                        logging.WARNING,
                        "completed_checkpoint_invalid",
                        source_name=source_name,
                        resource_id=resource_id,
                        expected_rows=declared_total,
                        actual_rows=actual_rows,
                        error=str(exc),
                        exception_type=type(exc).__name__,
                        recovery="restart_from_offset_zero",
                    )
                    output_path.unlink(missing_ok=True)
                    shutil.rmtree(checkpoint_dir, ignore_errors=True)
                    checkpoint_dir.mkdir(parents=True, exist_ok=True)
                    parts = []
                    offset = 0
                    page_number = 0
                    declared_total = None
                else:
                    return actual_rows

            else:
                parts = [p for p in parts if int(p.stem.split("_")[1]) < page_number]
                utils.log_event(
                    logging.INFO,
                    "pagination_resume",
                    source_name=source_name,
                    resource_id=resource_id,
                    extraction_route="datastore",
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
                if snapshot_restarts >= max_snapshot_restarts:
                    output_path.unlink(missing_ok=True)
                    shutil.rmtree(checkpoint_dir, ignore_errors=True)
                    raise RuntimeError(
                        f"{source_name}: DataStore total kept changing during "
                        f"extraction after {snapshot_restarts} full restart(s): "
                        f"{declared_total} -> {page_total}."
                    )

                snapshot_restarts += 1
                retry_delay_seconds = 5 * snapshot_restarts
                utils.log_event(
                    logging.WARNING,
                    "datastore_total_changed_restart",
                    source_name=source_name,
                    resource_id=resource_id,
                    previous_total=declared_total,
                    current_total=page_total,
                    attempt=snapshot_restarts,
                    max_restarts=max_snapshot_restarts,
                    retry_delay_seconds=retry_delay_seconds,
                    recovery="restart_from_offset_zero",
                )

                output_path.unlink(missing_ok=True)
                shutil.rmtree(checkpoint_dir, ignore_errors=True)
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                parts = []
                offset = 0
                page_number = 0
                declared_total = None
                time.sleep(retry_delay_seconds)
                continue

            if not records and offset < page_total:
                raise RuntimeError(
                    f"{source_name}: DataStore returned an empty page "
                    "before completion."
                )

            if records:
                frame = pd.DataFrame.from_records(records)
                frame = _normalize_source_schema(frame)
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
                resource_id=resource_id,
                extraction_route="datastore",
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
                    dtype=_source_csv_dtypes(),
                    keep_default_na=False,
                )
                if fmt == "csv"
                else self._iter_json_frames(temp_path)
            )
            for index, frame in enumerate(frames):
                frame = _normalize_source_schema(frame)
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
