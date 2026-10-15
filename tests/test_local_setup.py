"""Validate the local installation with a live one-source smoke test."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def _run(command: list[str], cwd: Path, env: dict[str, str]) -> None:
    """Run a CLI command and fail the test on a non-zero exit code."""
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"Command failed: {' '.join(command)}\n\n"
            f"stdout:\n{result.stdout}\n\n"
            f"stderr:\n{result.stderr}"
        )


def test_local_setup_end_to_end(tmp_path: Path) -> None:
    """Verify probe, one-source extraction, and local DuckDB loading."""
    if os.getenv("RUN_LIVE_TESTS") != "1":
        pytest.skip("Set RUN_LIVE_TESTS=1 to run the live setup test.")

    project_root = Path(__file__).resolve().parents[1]
    source_config = project_root / "open_data_config.yml"
    config = yaml.safe_load(source_config.read_text(encoding="utf-8"))

    config["warehouse"]["credentials"] = str(tmp_path / "toronto.duckdb")

    test_config = tmp_path / "open_data_config.yml"
    test_config.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )

    data_root = tmp_path / "data"
    env = os.environ.copy()
    env["RUN_LIVE_TESTS"] = "1"

    cli = [sys.executable, "-m", "toronto_housing_ingestion.ingestion"]

    _run(
        [
            *cli,
            "--config",
            str(test_config),
            "--source",
            "active_permits",
            "--probe-only",
            "--raw-root",
            str(data_root / "raw"),
            "--manifest-root",
            str(data_root / "manifests"),
            "--checkpoint-root",
            str(data_root / "checkpoints"),
            "--lock-file",
            str(data_root / ".ingestion.lock"),
        ],
        project_root,
        env,
    )

    _run(
        [
            *cli,
            "--config",
            str(test_config),
            "--source",
            "active_permits",
            "--full-refresh",
            "--raw-root",
            str(data_root / "raw"),
            "--manifest-root",
            str(data_root / "manifests"),
            "--checkpoint-root",
            str(data_root / "checkpoints"),
            "--lock-file",
            str(data_root / ".ingestion.lock"),
        ],
        project_root,
        env,
    )

    _run(
        [
            *cli,
            "--config",
            str(test_config),
            "--source",
            "active_permits",
            "--local-load",
            "--raw-root",
            str(data_root / "raw"),
            "--manifest-root",
            str(data_root / "manifests"),
            "--checkpoint-root",
            str(data_root / "checkpoints"),
            "--lock-file",
            str(data_root / ".ingestion.lock"),
        ],
        project_root,
        env,
    )

    parquet_files = list((data_root / "raw").rglob("data.parquet"))
    assert parquet_files

    assert (tmp_path / "toronto.duckdb").is_file()
    assert (tmp_path / "toronto.duckdb").stat().st_size > 0


"""
Run it as
RUN_LIVE_TESTS=1 pytest -q tests/test_local_setup.py
"""
