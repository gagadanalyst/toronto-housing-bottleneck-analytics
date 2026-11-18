"""Configuration models and validation for Toronto Housing Extraction."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class CKANConfig:
    base_url: str
    timeout_seconds: int
    retries: int
    page_size: int


@dataclass(frozen=True)
class SourceConfig:
    name: str
    title: str
    dataset_slug: str
    portal_url: str
    expected_cadence: str
    pagination_sort: str
    required_columns: tuple[str, ...]
    business_key: tuple[str, ...]
    min_row_count: int
    max_row_change_percent: float


@dataclass(frozen=True)
class ExtractionConfig:
    ckan: CKANConfig
    sources: dict[str, SourceConfig]


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string.")
    return value.strip()


def _string_list(value: Any, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item.strip() for item in value)
    ):
        raise ValueError(f"{name} must be a non-empty list of strings.")

    result = tuple(item.strip() for item in value)

    if len(result) != len(set(result)):
        raise ValueError(f"{name} contains duplicate values.")

    return result


def load_config(path: Path) -> ExtractionConfig:
    """Load and validate the extraction YAML configuration."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Cannot read configuration file: {path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML configuration: {path}") from exc

    if not isinstance(raw, dict):
        raise ValueError("Configuration must contain a YAML mapping.")

    ckan = raw.get("ckan")
    sources = raw.get("sources")

    if not isinstance(ckan, dict):
        raise ValueError("Configuration must contain a ckan mapping.")

    if not isinstance(sources, dict) or not sources:
        raise ValueError("Configuration must contain at least one source.")

    retries = ckan.get("retries")
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError("ckan.retries must be a non-negative integer.")

    ckan_config = CKANConfig(
        base_url=_string(ckan.get("base_url"), "ckan.base_url"),
        timeout_seconds=_positive_integer(
            ckan.get("timeout_seconds"), "ckan.timeout_seconds"
        ),
        retries=retries,
        page_size=_positive_integer(ckan.get("page_size"), "ckan.page_size"),
    )

    source_configs: dict[str, SourceConfig] = {}

    for name, spec in sources.items():
        if not isinstance(spec, dict):
            raise ValueError(f"Source {name!r} must be a YAML mapping.")

        required_columns = _string_list(
            spec.get("required_columns"),
            f"sources.{name}.required_columns",
        )
        business_key = _string_list(
            spec.get("business_key"),
            f"sources.{name}.business_key",
        )

        missing_keys = set(business_key) - set(required_columns)
        if missing_keys:
            raise ValueError(
                f"Source {name!r} business key contains columns "
                f"not listed as required: {sorted(missing_keys)}."
            )

        row_change = spec.get("max_row_change_percent")
        if isinstance(row_change, bool) or not isinstance(row_change, (int, float)):
            raise ValueError(f"Source {name!r} max_row_change_percent must be numeric.")
        if not 0 <= row_change <= 100:
            raise ValueError(
                f"Source {name!r} max_row_change_percent must be between 0 and 100."
            )

        source_configs[name] = SourceConfig(
            name=name,
            title=_string(spec.get("title"), f"sources.{name}.title"),
            dataset_slug=_string(
                spec.get("dataset_slug"), f"sources.{name}.dataset_slug"
            ),
            portal_url=_string(spec.get("portal_url"), f"sources.{name}.portal_url"),
            expected_cadence=_string(
                spec.get("expected_cadence"),
                f"sources.{name}.expected_cadence",
            ),
            pagination_sort=_string(
                spec.get("pagination_sort"),
                f"sources.{name}.pagination_sort",
            ),
            required_columns=required_columns,
            business_key=business_key,
            min_row_count=_positive_integer(
                spec.get("min_row_count"),
                f"sources.{name}.min_row_count",
            ),
            max_row_change_percent=float(row_change),
        )

    return ExtractionConfig(
        ckan=ckan_config,
        sources=source_configs,
    )
