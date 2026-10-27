from __future__ import annotations

import pytest

from dashboard.backend_config import resolve_dashboard_backend


def test_duckdb_is_the_default_backend() -> None:
    assert resolve_dashboard_backend() == "duckdb"


def test_streamlit_secret_can_select_snowflake() -> None:
    assert resolve_dashboard_backend(secrets_backend="snowflake") == "snowflake"


@pytest.mark.parametrize(
    ("environment_backend", "secrets_backend", "expected"),
    [
        ("duckdb", "snowflake", "duckdb"),
        ("snowflake", "duckdb", "snowflake"),
    ],
)
def test_environment_setting_takes_precedence(
    environment_backend: str,
    secrets_backend: str,
    expected: str,
) -> None:
    assert (
        resolve_dashboard_backend(
            environment_backend,
            secrets_backend,
        )
        == expected
    )


@pytest.mark.parametrize("configured", ["", "postgres", "invalid"])
def test_invalid_backend_setting_is_rejected(configured: str) -> None:
    with pytest.raises(ValueError, match="DASHBOARD_BACKEND must be either"):
        resolve_dashboard_backend(environment_backend=configured)
