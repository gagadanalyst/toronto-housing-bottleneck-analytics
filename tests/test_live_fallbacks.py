"""Live tests for CSV and JSON fallback downloads."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from toronto_housing_ingestion.ckan_client import TorontoCKANClient
from toronto_housing_ingestion.config import load_config


@pytest.fixture(scope="module")
def live_config():
    """Load project configuration when live tests are explicitly enabled."""
    if os.getenv("RUN_LIVE_TESTS") != "1":
        pytest.skip("Set RUN_LIVE_TESTS=1 to run live CKAN fallback tests.")
    return load_config(Path("open_data_config.yml"))


@pytest.mark.parametrize("fmt", ("csv", "json"))
def test_active_permits_fallback_download(
    live_config, tmp_path: Path, fmt: str
) -> None:
    """Verify our configured fallback can download and materialize each format."""
    source = next(s for s in live_config.sources if s.name == "active_permits")

    with TorontoCKANClient(
        live_config.ckan_base_url,
        live_config.timeout_seconds,
        live_config.retries,
    ) as client:
        package = client.package_show(source.dataset_slug)
        resources = [
            resource
            for resource in client.fallback_resources(package)
            if str(resource.get("format", "")).lower() == fmt
        ]
        if not resources:
            pytest.skip(f"No {fmt.upper()} fallback resource is published.")

        output = tmp_path / f"active_permits_{fmt}.parquet"
        row_count = client.download_fallback(
            resource=resources[0],
            output_path=output,
            source_name=source.name,
            required_columns=source.required_columns,
            date_columns=source.date_columns,
        )

    assert output.exists()
    assert row_count > 0
