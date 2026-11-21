from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.workflow_push_routing import (
    get_changed_paths,
    should_run_push,
)


def test_loading_runs_for_loading_code_changes():
    assert should_run_push(
        "loading",
        ("src/toronto_housing_ingestion/loader.py",),
    )


def test_loading_skips_extraction_changes():
    assert not should_run_push(
        "loading",
        (
            "extraction/src/toronto_housing_extraction/snapshots.py",
            "scripts/mode1_state.py",
        ),
    )


def test_loading_skips_workflow_only_changes():
    assert not should_run_push(
        "loading",
        (".github/workflows/mode1-incremental-loader.yml",),
    )


def test_loading_skips_routing_helper_only_changes():
    assert not should_run_push(
        "loading",
        ("scripts/workflow_push_routing.py",),
    )


def test_transformation_runs_for_dbt_changes():
    assert should_run_push(
        "transformation",
        ("dbt/models/marts/example.sql",),
    )


def test_transformation_skips_upstream_loading_changes():
    assert not should_run_push(
        "transformation",
        (
            "scripts/mode1_state.py",
            "dbt/models/marts/example.sql",
        ),
    )


def test_transformation_skips_workflow_only_changes():
    assert not should_run_push(
        "transformation",
        (".github/workflows/transformation-dbt-serving.yml",),
    )


def test_empty_or_unrelated_changes_fail_closed():
    assert not should_run_push("loading", ())
    assert not should_run_push("transformation", ())
    assert not should_run_push("loading", ("README.md",))


def test_changed_paths_come_from_actual_git_commit_range(tmp_path: Path):
    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(tmp_path), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    git("init", "-q")
    git("config", "user.name", "Workflow Routing Test")
    git("config", "user.email", "workflow-routing@example.invalid")

    first_file = tmp_path / "README.md"
    first_file.write_text("baseline\n", encoding="utf-8")
    git("add", "README.md")
    git("commit", "-m", "baseline")
    before = git("rev-parse", "HEAD")

    changed_file = tmp_path / "src/toronto_housing_ingestion/sample.py"
    changed_file.parent.mkdir(parents=True)
    changed_file.write_text("value = 1\n", encoding="utf-8")
    git("add", "src/toronto_housing_ingestion/sample.py")
    git("commit", "-m", "change loader")
    after = git("rev-parse", "HEAD")

    assert get_changed_paths(tmp_path, before, after) == (
        "src/toronto_housing_ingestion/sample.py",
    )


def test_invalid_commit_range_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="Invalid or unavailable before"):
        get_changed_paths(tmp_path, "not-a-sha", "a" * 40)


@pytest.mark.parametrize(
    ("workflow", "mode", "output"),
    [
        (
            ".github/workflows/mode1-incremental-loader.yml",
            "loading",
            "should_load",
        ),
        (
            ".github/workflows/transformation-dbt-serving.yml",
            "transformation",
            "should_transform",
        ),
    ],
)
def test_workflow_uses_full_git_diff_router(workflow, mode, output):
    root = Path(__file__).resolve().parents[1]
    content = (root / workflow).read_text(encoding="utf-8")

    assert "fetch-depth: 0" in content
    assert "BEFORE_SHA: ${{ github.event.before }}" in content
    assert "AFTER_SHA: ${{ github.sha }}" in content
    assert f"--mode {mode}" in content
    assert "scripts/workflow_push_routing.py" in content
    assert output in content
    assert "COMMITS_JSON" not in content


def test_github_output_names_match_workflow_job_outputs():
    from scripts.workflow_push_routing import OUTPUT_NAMES

    assert OUTPUT_NAMES == {
        "loading": "should_load",
        "transformation": "should_transform",
    }


def test_cli_writes_only_valid_github_output_to_stdout(monkeypatch, capsys):
    import sys

    from scripts import workflow_push_routing

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "workflow_push_routing.py",
            "--mode",
            "loading",
            "--repo",
            ".",
            "--before",
            "a" * 40,
            "--after",
            "b" * 40,
        ],
    )
    monkeypatch.setattr(
        workflow_push_routing,
        "get_changed_paths",
        lambda *args: ("src/toronto_housing_ingestion/loader.py",),
    )

    assert workflow_push_routing.main() == 0

    captured = capsys.readouterr()
    assert captured.out == "should_load=true\n"
    assert "Push routing inspected" in captured.err


def test_transformation_runs_for_snowflake_serving_schema_changes():
    assert should_run_push(
        "transformation",
        (".github/workflows/snowflake-serving-schema.yml",),
    )
