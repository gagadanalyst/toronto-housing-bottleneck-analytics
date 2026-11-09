"""Focused tests for joint recovery metadata validation."""

from __future__ import annotations

import sys
from types import ModuleType

if "snowflake.connector" not in sys.modules:
    try:
        import snowflake.connector  # noqa: F401
    except ModuleNotFoundError:
        package = ModuleType("snowflake")
        package.__path__ = []
        connector = ModuleType("snowflake.connector")
        connector.connect = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("Unit tests must not connect to Snowflake")
        )
        package.connector = connector
        sys.modules["snowflake"] = package
        sys.modules["snowflake.connector"] = connector

import pytest
from dlt.common.versioned_state import compress_state, decompress_state

from scripts import validate_scd2_replay_recovery as validation


def _metadata_fixture():
    active_pipeline = "toronto_active_permits_scd2"
    cleared_pipeline = "toronto_cleared_permits_scd2"
    loads = [
        (f"active-load-{index}", active_pipeline, 0, "active-schema-v1")
        for index in range(28)
    ] + [
        (f"cleared-load-{index}", cleared_pipeline, 0, "cleared-schema-v1")
        for index in range(28)
    ]
    states = []
    for index in range(28):
        active_state = compress_state(
            {
                "pipeline_name": active_pipeline,
                "dataset_name": "raw",
                "default_schema_name": active_pipeline,
                "schema_names": [active_pipeline],
                "state": True,
            }
        )
        cleared_state = compress_state(
            {
                "pipeline_name": cleared_pipeline,
                "dataset_name": "raw",
                "default_schema_name": cleared_pipeline,
                "schema_names": [cleared_pipeline],
                "state": True,
            }
        )
        states.append((active_pipeline, active_state, f"active-load-{index}"))
        states.append((cleared_pipeline, cleared_state, f"cleared-load-{index}"))
    versions = [
        (active_pipeline, "active-schema-v1"),
        (cleared_pipeline, "cleared-schema-v1"),
    ]
    return loads, states, versions


def test_metadata_validator_requires_28_completed_loads_per_pipeline():
    loads, states, versions = _metadata_fixture()
    result = validation._metadata_counts(loads, states, versions, "test", "raw")

    assert result["completed_loads"] == 56
    assert result["completed_loads_per_pipeline"] == {
        "toronto_active_permits_scd2": 28,
        "toronto_cleared_permits_scd2": 28,
    }
    assert result["distinct_state_load_ids_per_pipeline"] == {
        "toronto_active_permits_scd2": 28,
        "toronto_cleared_permits_scd2": 28,
    }


def test_metadata_validator_rejects_duplicate_pipeline_state_load_ids():
    loads, states, versions = _metadata_fixture()
    active_state = compress_state(
        {
            "pipeline_name": "toronto_active_permits_scd2",
            "dataset_name": "raw",
            "default_schema_name": "toronto_active_permits_scd2",
            "schema_names": ["toronto_active_permits_scd2"],
            "state": True,
        }
    )
    states = [
        ("toronto_active_permits_scd2", active_state, "active-load-0")
        for _ in range(28)
    ] + states[28:]
    with pytest.raises(ValueError, match="duplicate pipeline-state checkpoint"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_incomplete_dlt_load_reference():
    loads, states, versions = _metadata_fixture()
    loads.append(("failed-load", "toronto_active_permits_scd2", 1, "active-schema-v1"))
    active_state = compress_state(
        {
            "pipeline_name": "toronto_active_permits_scd2",
            "dataset_name": "raw",
            "default_schema_name": "toronto_active_permits_scd2",
            "schema_names": ["toronto_active_permits_scd2"],
            "state": True,
        }
    )
    states[0] = ("toronto_active_permits_scd2", active_state, "failed-load")
    with pytest.raises(ValueError, match="not linked to a completed load"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_missing_schema_version():
    loads, states, _versions = _metadata_fixture()
    versions = [
        ("another_schema", "active-schema-v1"),
        ("another_schema", "cleared-schema-v1"),
    ]
    with pytest.raises(ValueError, match="missing schema version"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_pipeline_without_any_state_checkpoint():
    loads, states, versions = _metadata_fixture()
    states = [row for row in states if row[0] != "toronto_cleared_permits_scd2"]
    with pytest.raises(ValueError, match="has no pipeline-state checkpoint"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_empty_pipeline_state_payload():
    loads, states, versions = _metadata_fixture()
    states[0] = ("toronto_active_permits_scd2", "{}", "load-0")
    with pytest.raises(ValueError, match="empty or non-object DLT pipeline state"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_pipeline_state_for_wrong_dataset():
    loads, states, versions = _metadata_fixture()
    with pytest.raises(ValueError, match="dataset_name does not match target"):
        validation._metadata_counts(loads, states, versions, "test", "other_schema")


def test_metadata_validator_rejects_pipeline_state_for_wrong_pipeline():
    loads, states, versions = _metadata_fixture()
    wrong = compress_state(
        {
            "pipeline_name": "another_pipeline",
            "dataset_name": "raw",
            "default_schema_name": "toronto_active_permits_scd2",
            "schema_names": ["toronto_active_permits_scd2"],
            "state": True,
        }
    )
    states[0] = ("toronto_active_permits_scd2", wrong, "active-load-0")
    with pytest.raises(ValueError, match="pipeline_name does not match"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_malformed_pipeline_state_payload():
    loads, states, versions = _metadata_fixture()
    states[0] = ("toronto_active_permits_scd2", "not-json", "load-0")
    with pytest.raises(ValueError, match="not valid JSON"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_pipeline_state_for_other_pipeline_load():
    loads, states, versions = _metadata_fixture()
    cleared_state = states[1][1]
    states[1] = ("toronto_cleared_permits_scd2", cleared_state, "active-load-0")
    with pytest.raises(ValueError, match="belonging to 'toronto_active_permits_scd2'"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_accepts_completed_load_without_its_own_state_checkpoint():
    loads, states, versions = _metadata_fixture()
    loads.append(
        ("active-orphan-success", "toronto_active_permits_scd2", 0, "active-schema-v1")
    )

    result = validation._metadata_counts(loads, states, versions, "test", "raw")

    assert result["completed_loads"] == 57
    assert result["completed_loads_per_pipeline"] == {
        "toronto_active_permits_scd2": 29,
        "toronto_cleared_permits_scd2": 28,
    }
    assert result["distinct_state_load_ids_per_pipeline"] == {
        "toronto_active_permits_scd2": 28,
        "toronto_cleared_permits_scd2": 28,
    }


def test_metadata_validator_accepts_sparse_state_checkpoints_for_both_pipelines():
    loads, states, versions = _metadata_fixture()
    states = [states[0], states[1]]

    result = validation._metadata_counts(loads, states, versions, "test", "raw")

    assert result["distinct_state_load_ids_per_pipeline"] == {
        "toronto_active_permits_scd2": 1,
        "toronto_cleared_permits_scd2": 1,
    }


def test_metadata_validator_rejects_fewer_than_28_completed_loads_for_a_pipeline():
    loads, states, versions = _metadata_fixture()
    loads = [row for row in loads if row[0] != "active-load-0"]

    with pytest.raises(
        ValueError,
        match="toronto_active_permits_scd2 has fewer than 28 completed DLT loads",
    ):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_unknown_dlt_load_schema():
    loads, states, versions = _metadata_fixture()
    loads[0] = ("active-load-0", "unexpected_schema", 0, "active-schema-v1")
    with pytest.raises(ValueError, match="unexpected DLT load schema"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_schema_version_hash_mismatch_for_same_schema():
    loads, states, versions = _metadata_fixture()
    versions[0] = ("toronto_active_permits_scd2", "wrong-version-hash")
    with pytest.raises(ValueError, match="missing schema version"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_metadata_validator_rejects_state_with_wrong_default_schema():
    loads, states, versions = _metadata_fixture()
    payload = decompress_state(states[0][1])
    payload["default_schema_name"] = "toronto_cleared_permits_scd2"
    states[0] = (states[0][0], compress_state(payload), states[0][2])
    with pytest.raises(ValueError, match="default_schema_name does not match"):
        validation._metadata_counts(loads, states, versions, "test", "raw")


def test_state_payload_decodes_dlt_compressed_state():
    payload = {
        "pipeline_name": "toronto_active_permits_scd2",
        "dataset_name": "raw",
        "default_schema_name": "toronto_active_permits_scd2",
        "schema_names": ["toronto_active_permits_scd2"],
    }
    assert (
        validation._state_payload(
            compress_state(payload), "toronto_active_permits_scd2", "test"
        )
        == payload
    )


def _ledger_fixture():
    rows = []
    for run_id, attempt in validation.EXPECTED:
        release_tag = "toronto-raw-snapshot-archive-2026-10"
        asset_name = f"toronto-raw-snapshot-bundle-{run_id}-{attempt}.zip"
        for source_name in validation.SOURCE_NAMES:
            rows.append(
                (
                    f"{run_id}:{attempt}",
                    run_id,
                    attempt,
                    "2026-10-27T14:00:00+00:00",
                    source_name,
                    "succeeded",
                    "a" * 40,
                    release_tag,
                    asset_name,
                    None,
                )
            )
    return rows


def test_ledger_validator_accepts_exact_28_snapshot_56_source_rows():
    records = validation._ledger_records(_ledger_fixture(), "synthetic")
    assert len(records) == 56


def test_ledger_validator_rejects_missing_source_row():
    with pytest.raises(ValueError, match="expected 56 unique ledger entries"):
        validation._ledger_records(_ledger_fixture()[:-1], "synthetic")


def test_ledger_validator_rejects_duplicate_source_row():
    rows = _ledger_fixture()
    with pytest.raises(ValueError, match="duplicate ledger entry"):
        validation._ledger_records(rows + rows[:1], "synthetic")


def test_ledger_validator_rejects_failed_source_status():
    rows = _ledger_fixture()
    rows[0] = (*rows[0][:5], "failed", *rows[0][6:])
    with pytest.raises(ValueError, match="source load is not successful"):
        validation._ledger_records(rows, "synthetic")


def test_ledger_validator_rejects_invalid_archive_commit():
    rows = _ledger_fixture()
    rows[0] = (*rows[0][:6], "g" * 40, *rows[0][7:])
    with pytest.raises(ValueError, match="invalid source commit"):
        validation._ledger_records(rows, "synthetic")
