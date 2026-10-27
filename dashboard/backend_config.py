"""Pure configuration helpers for the dashboard backend."""

from __future__ import annotations


def resolve_dashboard_backend(
    environment_backend: str | None = None,
    secrets_backend: str | None = None,
) -> str:
    """Resolve the backend, preferring the environment over Streamlit secrets."""
    configured = (
        environment_backend if environment_backend is not None else secrets_backend
    )
    backend = "duckdb" if configured is None else str(configured).strip().lower()

    if backend not in {"duckdb", "snowflake"}:
        raise ValueError("DASHBOARD_BACKEND must be either 'duckdb' or 'snowflake'.")

    return backend
