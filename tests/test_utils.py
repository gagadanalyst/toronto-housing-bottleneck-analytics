import json
from pathlib import Path

import pandas as pd

from toronto_housing_ingestion import utils


def test_atomic_json_and_sha(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    utils.atomic_write_json(path, {"ok": True})
    assert json.loads(path.read_text())["ok"] is True
    assert len(utils.sha256_file(path)) == 64


def test_validate_required_columns() -> None:
    frame = pd.DataFrame({"PERMIT_NUM": [1]})
    try:
        utils.validate_required_columns(frame, ("permit_num", "issued_date"), "active")
    except ValueError as exc:
        assert "issued_date" in str(exc)
    else:
        raise AssertionError("Expected missing-column error")


def test_validate_data_types_allows_nulls() -> None:
    frame = pd.DataFrame({"APPLICATION_DATE": [None, "2025-01-01"]})
    utils.validate_data_types(frame, ("APPLICATION_DATE",), "active")


def test_checkpoint_matches() -> None:
    assert utils.checkpoint_matches(
        {"resource_id": "1", "fingerprint": {"x": "y"}}, "1", {"x": "y"}
    )


def test_duplicate_keys_are_warnings(tmp_path: Path) -> None:
    """Duplicate natural keys are reported without raising an exception."""
    pytest = __import__("pytest")
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")

    path = tmp_path / "data.parquet"
    frame = pd.DataFrame({"permit_num": [1, 1], "revision_num": [0, 0]})
    frame.to_parquet(path, index=False)

    warnings = utils.key_quality_warnings(
        path, ("permit_num", "revision_num"), "active_permits"
    )

    assert warnings[0]["type"] == "duplicate_key"
    assert warnings[0]["rows"] == 1
