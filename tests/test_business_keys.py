"""Validate the configured business keys on extracted permit snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from toronto_housing_ingestion import utils

EXPECTED_KEY = ("permit_num", "revision_num", "permit_type")
SOURCES = ("active_permits", "cleared_permits")


def _latest_manifest(source_name: str) -> Path | None:
    paths = sorted(Path("data/manifests", source_name).glob("*.json"), reverse=True)
    return paths[0] if paths else None


@pytest.mark.parametrize("source_name", SOURCES)
def test_permit_business_key_is_valid(source_name: str) -> None:
    """Check the configured business key in the latest snapshot."""
    manifest_path = _latest_manifest(source_name)
    if manifest_path is None:
        pytest.skip("No extracted snapshot is available.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert tuple(manifest["primary_key"]) == EXPECTED_KEY
    assert manifest["warehouse_status"] == "loaded"

    warnings = utils.key_quality_warnings(
        Path(manifest["raw_path"]), EXPECTED_KEY, source_name
    )
    assert warnings == []
