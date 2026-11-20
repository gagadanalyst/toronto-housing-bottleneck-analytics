from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import snapshot_replay_baseline as baseline


@pytest.fixture(autouse=True)
def _enable_snowflake_operations(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "true")


def test_duckdb_baseline_is_sha256_and_detects_file_mutation(tmp_path):
    database = tmp_path / "production.duckdb"
    database.write_bytes(b"production-baseline")
    expected = baseline.duckdb_signature(database)
    assert expected["backend"] == "duckdb"
    assert expected["size_bytes"] == len(b"production-baseline")
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(expected), encoding="utf-8")
    baseline.verify(path, baseline.duckdb_signature(database))

    database.write_bytes(b"changed-production-baseline")
    with pytest.raises(RuntimeError, match="changed after"):
        baseline.verify(path, baseline.duckdb_signature(database))


def test_duckdb_baseline_refuses_missing_or_empty_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        baseline.duckdb_signature(tmp_path / "missing.duckdb")
    empty = tmp_path / "empty.duckdb"
    empty.touch()
    with pytest.raises(FileNotFoundError):
        baseline.duckdb_signature(empty)


def test_snowflake_signature_checks_context_and_aggregates(monkeypatch):
    class Cursor:
        def __init__(self):
            self.results = [
                (
                    "TORONTO_INGEST_ROLE",
                    "TORONTO",
                    "RAW",
                    "TORONTO_XS_WH",
                    "BIHASTE",
                    "TU44125",
                ),
                (100, 80, 20, 101),
                (200, 190, 10, 202),
                (56, 28, 303),
                (2, 2, 404),
            ]
            self.queries = []

        def execute(self, query):
            self.queries.append(query)

        def fetchone(self):
            return self.results.pop(0)

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cursor_value = Cursor()
            self.closed = False

        def cursor(self):
            return self.cursor_value

        def close(self):
            self.closed = True

    connection = Connection()
    temporary = SimpleNamespace(cleanup=lambda: None)
    monkeypatch.setattr(
        baseline, "_snowflake_connect", lambda _uri: (connection, temporary)
    )
    value = baseline.snowflake_signature("unused")
    assert value["account"] == "bihaste-tu44125"
    assert value["tables"]["RAW_ACTIVE_PERMITS"]["hash_agg"] == "101"
    assert value["tables"]["RAW_CLEARED_PERMITS"]["hash_agg"] == "202"
    assert value["snapshot_ledger"]["snapshots"] == 28
    assert value["dlt_pipeline_state"] == {
        "rows": 2,
        "pipelines": 2,
        "hash_agg": "404",
    }
    assert "_DLT_LOAD_ID" in connection.cursor_value.queries[-1]
    assert "UPDATED_AT" in connection.cursor_value.queries[-2]
    ledger_query = connection.cursor_value.queries[-2].upper()
    assert "FORMAT_VERSION" not in ledger_query
    assert "ARCHIVE_SHA256" not in ledger_query
    assert len(connection.cursor_value.queries) == 5
    assert connection.closed


def test_snowflake_signature_rejects_unexpected_context(monkeypatch):
    class Cursor:
        def execute(self, _query):
            pass

        def fetchone(self):
            return ("PUBLIC", "TORONTO", "RAW", "TORONTO_XS_WH", "BIHASTE", "TU44125")

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(
        baseline,
        "_snowflake_connect",
        lambda _uri: (Connection(), SimpleNamespace(cleanup=lambda: None)),
    )
    with pytest.raises(RuntimeError, match="canonical ingest context"):
        baseline.snowflake_signature("unused")


def test_snowflake_signature_rejects_incomplete_dlt_pipeline_state(monkeypatch):
    class Cursor:
        def __init__(self):
            self.results = [
                (
                    "TORONTO_INGEST_ROLE",
                    "TORONTO",
                    "RAW",
                    "TORONTO_XS_WH",
                    "BIHASTE",
                    "TU44125",
                ),
                (100, 80, 20, 101),
                (200, 190, 10, 202),
                (56, 28, 303),
                (2, 1, 404),
            ]

        def execute(self, _query):
            pass

        def fetchone(self):
            return self.results.pop(0)

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(
        baseline,
        "_snowflake_connect",
        lambda _uri: (Connection(), SimpleNamespace(cleanup=lambda: None)),
    )
    with pytest.raises(RuntimeError, match="DLT pipeline state"):
        baseline.snowflake_signature("unused")


def test_mode1_workflow_fingerprints_production_before_candidate_copy_and_promotion():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "snapshot-replay.yml"
    ).read_text(encoding="utf-8")

    assert "options: [rebuild, incremental]\n        default: rebuild" in workflow
    assert workflow.count("--baseline-snapshot-id") == 2
    assert workflow.count("--baseline-snapshot-count") == 2

    duck_capture = workflow.index(
        "--output data/staging/duckdb/production-baseline.json"
    )
    duck_copy = workflow.index("python scripts/prepare_duckdb_candidate.py")
    duck_replay = workflow.index("id: replay", duck_copy)
    duck_verify = workflow.index(
        "--expected data/staging/duckdb/production-baseline.json"
    )
    duck_promote = workflow.index(
        "python scripts/promote_duckdb_candidate.py", duck_verify
    )
    assert duck_capture < duck_copy < duck_replay < duck_verify < duck_promote
    assert "id: prepare_duckdb_candidate" in workflow

    snow_capture = workflow.index(
        "--output data/staging/snowflake/production-baseline.json"
    )
    snow_clone = workflow.index("python scripts/loader_prepare_snowflake_raw.py")
    snow_replay = workflow.index('--mode "$REPLAY_MODE"', snow_clone)
    snow_verify = workflow.index(
        "--expected data/staging/snowflake/production-baseline.json"
    )
    snow_promote = workflow.index(
        "python scripts/loader_promote_snowflake_raw.py", snow_verify
    )
    assert snow_capture < snow_clone < snow_replay < snow_verify < snow_promote


def test_snowflake_baseline_capture_rejects_disabled_gate(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "false")

    with pytest.raises(PermissionError, match="SNOWFLAKE_OPERATIONS_ENABLED"):
        baseline.capture(
            "snowflake",
            database=None,
            credentials="unused",
        )


def test_mode1_dry_run_never_promotes_or_publishes():
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "snapshot-replay.yml"
    ).read_text(encoding="utf-8")

    assert "dry_run:" in workflow
    assert workflow.count("DRY_RUN: ${{ inputs.dry_run == true }}") == 2

    def step(name):
        start = workflow.index(f"      - name: {name}")
        end = workflow.find("\n      - name: ", start + 1)
        return workflow[start:] if end == -1 else workflow[start:end]

    assert "--dry-run" in step("Prepare the Mode 1 Snowflake candidate clone")
    assert "--validate-only" in step(
        "Validate isolated Snowflake candidate without promotion"
    )

    duckdb_validation = step("Validate complete dbt candidate before promotion")
    assert 'candidate="${DBT_DUCKDB_PATH:-${REPLAY_DATABASE:-}}"' in duckdb_validation

    assert "env.DRY_RUN != 'true'" in step(
        "Promote the fully validated DuckDB and dbt candidate"
    )
    assert "env.DRY_RUN != 'true'" in step(
        "Promote the validated Mode 1 Snowflake candidate"
    )
    assert "env.DRY_RUN != 'true'" in step(
        "Publish durable DuckDB, Parquet and dbt assets"
    )
    assert "env.DRY_RUN != 'true'" in step("Upload GitHub Pages artifact")

    cleanup = step("Clean up Mode 1 Snowflake dry-run candidate")
    assert "always()" in cleanup
    assert "steps.prepare_snowflake_candidate.outcome == 'success'" in cleanup
    assert "--drop-candidate" in cleanup

    assert "inputs.dry_run != true" in workflow[workflow.index("  deploy-dbt-docs:") :]
