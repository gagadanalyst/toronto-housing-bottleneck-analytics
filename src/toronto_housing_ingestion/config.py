"""Configuration models and validation for the ingestion job."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for one Toronto Open Data source."""

    name: str
    title: str
    dataset_slug: str
    role: str
    expected_cadence: str
    portal_url: str
    required_columns: tuple[str, ...]
    date_columns: tuple[str, ...]
    pagination_sort: str | None
    warehouse_table: str
    warehouse_load: bool
    primary_key: tuple[str, ...]
    type_hints: dict[str, dict[str, Any]]
    warehouse_since: str | None = None
    warehouse_date_column: str | None = None
    min_row_count: int | None = None
    max_row_change_percent: float | None = None
    warehouse_strategy: str = "scd2"


@dataclass(frozen=True)
class AppConfig:
    """Validated application-wide configuration."""

    ckan_base_url: str
    timeout_seconds: int
    retries: int
    page_size: int
    warehouse_kind: str
    warehouse_credentials: str
    dataset_name: str
    sources: tuple[SourceConfig, ...]


def _positive_int(value: Any, name: str) -> int:
    """Return a positive integer or raise ``ValueError``."""
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return result


def validate_config(raw: dict[str, Any]) -> None:
    """Validate the YAML structure used by the ingestion CLI."""
    if not isinstance(raw, dict):
        raise ValueError("Configuration must be a YAML mapping.")

    ckan = raw.get("ckan")
    if not isinstance(ckan, dict):
        raise ValueError("Configuration must contain a 'ckan' mapping.")
    for key in ("base_url", "timeout_seconds", "retries", "page_size"):
        if key not in ckan:
            raise ValueError(f"Missing CKAN setting: {key}")
    if not str(ckan["base_url"]).startswith("https://"):
        raise ValueError("ckan.base_url must use HTTPS.")
    _positive_int(ckan["timeout_seconds"], "ckan.timeout_seconds")
    _positive_int(ckan["retries"], "ckan.retries")
    _positive_int(ckan["page_size"], "ckan.page_size")

    warehouse = raw.get("warehouse")
    if not isinstance(warehouse, dict):
        raise ValueError("Configuration must contain a 'warehouse' mapping.")
    kind = str(warehouse.get("kind", "duckdb")).lower()
    if kind not in {"duckdb", "snowflake"}:
        raise ValueError("warehouse.kind must be 'duckdb' or 'snowflake'.")
    if not warehouse.get("credentials"):
        raise ValueError("warehouse.credentials is required.")
    if not warehouse.get("dataset_name"):
        raise ValueError("warehouse.dataset_name is required.")

    sources = raw.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError("Configuration must contain at least one source.")

    slugs: set[str] = set()
    tables: set[str] = set()
    for name, spec in sources.items():
        if not isinstance(spec, dict):
            raise ValueError(f"Source '{name}' must be a mapping.")

        required = {
            "title",
            "dataset_slug",
            "role",
            "expected_cadence",
            "portal_url",
            "required_columns",
            "pagination_sort",
            "warehouse_table",
            "warehouse_load",
            "primary_key",
        }
        missing = required - spec.keys()
        if missing:
            raise ValueError(f"Source '{name}' is missing: {sorted(missing)}")

        slug = str(spec["dataset_slug"])
        table = str(spec["warehouse_table"])
        if slug in slugs:
            raise ValueError(f"Duplicate dataset_slug: {slug}")
        if table in tables:
            raise ValueError(f"Duplicate warehouse_table: {table}")
        slugs.add(slug)
        tables.add(table)

        if not str(spec["portal_url"]).startswith("https://"):
            raise ValueError(f"Source '{name}' portal_url must use HTTPS.")
        if not isinstance(spec["warehouse_load"], bool):
            raise ValueError(f"Source '{name}' warehouse_load must be true or false.")

        strategy = str(spec.get("warehouse_strategy", "scd2")).lower()

        if strategy not in {"scd2", "replace"}:
            raise ValueError(
                f"Source '{name}' warehouse_strategy must be 'scd2' or 'replace'."
            )

        for field in ("required_columns", "primary_key"):
            value = spec[field]
            if not isinstance(value, list) or any(
                not isinstance(v, str) for v in value
            ):
                raise ValueError(f"Source '{name}' {field} must be a list of strings.")

        date_columns = spec.get("date_columns", [])
        if not isinstance(date_columns, list) or any(
            not isinstance(v, str) for v in date_columns
        ):
            raise ValueError(f"Source '{name}' date_columns must be a list of strings.")

        warehouse_since = spec.get("warehouse_since")
        warehouse_date_column = spec.get("warehouse_date_column")
        if (warehouse_since is None) != (warehouse_date_column is None):
            raise ValueError(
                f"Source '{name}' must set both warehouse_since and "
                "warehouse_date_column, or neither."
            )
        if warehouse_since is not None:
            try:
                date.fromisoformat(str(warehouse_since))
            except ValueError as exc:
                raise ValueError(
                    f"Source '{name}' warehouse_since must be YYYY-MM-DD."
                ) from exc
            if not isinstance(warehouse_date_column, str) or not warehouse_date_column:
                raise ValueError(f"Source '{name}' warehouse_date_column is required.")

        if (
            spec.get("warehouse_load")
            and strategy == "scd2"
            and not spec["primary_key"]
        ):
            raise ValueError(
                f"Source '{name}' needs primary_key when warehouse_strategy is 'scd2'."
            )
        if spec.get("warehouse_load") and not spec.get("pagination_sort"):
            raise ValueError(
                f"Source '{name}' needs pagination_sort for stable extraction."
            )

        min_rows = spec.get("min_row_count")
        if min_rows is not None and int(min_rows) < 0:
            raise ValueError(f"Source '{name}' min_row_count must be >= 0.")

        max_change = spec.get("max_row_change_percent")
        if max_change is not None and not 0 <= float(max_change) <= 100:
            raise ValueError(
                f"Source '{name}' max_row_change_percent must be between 0 and 100."
            )

        type_hints = spec.get("type_hints", {})
        if not isinstance(type_hints, dict):
            raise ValueError(f"Source '{name}' type_hints must be a mapping.")


def load_config(path: Path) -> AppConfig:
    """Load and validate a YAML configuration file.

    The project deliberately targets Python 3.14 so local runs match CI.
    """
    if sys.version_info[:2] != (3, 14):
        raise RuntimeError(
            f"This project requires Python 3.14.x; detected {sys.version.split()[0]}."
        )

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    validate_config(raw)
    ckan = raw["ckan"]
    warehouse = raw["warehouse"]

    sources = tuple(
        SourceConfig(
            name=name,
            title=str(spec["title"]),
            dataset_slug=str(spec["dataset_slug"]),
            role=str(spec["role"]),
            expected_cadence=str(spec["expected_cadence"]),
            portal_url=str(spec["portal_url"]),
            required_columns=tuple(spec["required_columns"]),
            date_columns=tuple(spec.get("date_columns", [])),
            pagination_sort=spec.get("pagination_sort"),
            warehouse_table=str(spec["warehouse_table"]),
            warehouse_load=bool(spec["warehouse_load"]),
            primary_key=tuple(spec["primary_key"]),
            type_hints=dict(spec.get("type_hints", {})),
            warehouse_since=spec.get("warehouse_since"),
            warehouse_date_column=spec.get("warehouse_date_column"),
            min_row_count=(
                int(spec["min_row_count"])
                if spec.get("min_row_count") is not None
                else None
            ),
            max_row_change_percent=(
                float(spec["max_row_change_percent"])
                if spec.get("max_row_change_percent") is not None
                else None
            ),
            warehouse_strategy=str(spec.get("warehouse_strategy", "scd2")).lower(),
        )
        for name, spec in raw["sources"].items()
    )

    return AppConfig(
        ckan_base_url=str(ckan["base_url"]),
        timeout_seconds=int(ckan["timeout_seconds"]),
        retries=int(ckan["retries"]),
        page_size=int(ckan["page_size"]),
        warehouse_kind=os.getenv(
            "TORONTO_WAREHOUSE_KIND",
            str(warehouse.get("kind", "duckdb")),
        ).lower(),
        warehouse_credentials=os.getenv(
            "TORONTO_WAREHOUSE_CREDENTIALS",
            str(warehouse["credentials"]),
        ),
        dataset_name=os.getenv(
            "TORONTO_WAREHOUSE_DATASET",
            str(warehouse["dataset_name"]),
        ),
        sources=sources,
    )
