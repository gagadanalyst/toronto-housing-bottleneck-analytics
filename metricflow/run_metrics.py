#!/usr/bin/env python3
"""Execute the dbt v2 semantic metrics and publish a compact JSON result file."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = ROOT / "dbt"
ACTIVE_METRICS = ",".join(
    (
        "active_permit_revision_count",
        "eligible_active_permit_revision_count",
        "median_active_permit_age_days",
        "p90_active_permit_age_days",
    )
)
CLEARED_METRICS = ",".join(
    (
        "cleared_permit_revision_count",
        "closed_application_to_issue_valid_revision_count",
        "closed_application_to_completion_valid_revision_count",
        "closed_issue_to_completion_valid_revision_count",
        "median_application_to_issue_days",
        "p90_application_to_issue_days",
        "median_application_to_completion_days",
        "p90_application_to_completion_days",
        "median_issue_to_completion_days",
        "p90_issue_to_completion_days",
    )
)


def run(command: list[str], env: dict[str, str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=DBT_PROJECT, env=env, check=True)


def timestamp_text(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def warehouse_refresh_metadata(database: Path) -> dict[str, str | None]:
    import duckdb

    with duckdb.connect(str(database), read_only=True) as connection:
        available = {
            (str(row[0]).lower(), str(row[1]).lower())
            for row in connection.execute(
                "SELECT table_schema, table_name FROM information_schema.tables"
            ).fetchall()
        }
        if ("metadata", "dashboard_refresh_metadata") not in available:
            raise RuntimeError(
                "The published DuckDB warehouse has no dashboard refresh metadata."
            )
        row = connection.execute(
            "SELECT last_successful_refresh_utc, source_commit, workflow_run_id, "
            "source_mode1_run_id "
            "FROM metadata.dashboard_refresh_metadata WHERE refresh_id = 1"
        ).fetchone()
    if not row or not row[2]:
        raise RuntimeError(
            "The published DuckDB warehouse has no successful refresh run identity."
        )
    return {
        "warehouse_refresh_timestamp_utc": timestamp_text(row[0]),
        "warehouse_source_commit": str(row[1]) if row[1] is not None else None,
        "warehouse_refresh_run_id": str(row[2]),
        "warehouse_source_mode1_run_id": str(row[3]) if row[3] is not None else None,
    }


def parse_csv_value(value: str) -> Any:
    value = value.strip()
    if not value or value.lower() in {"null", "none", "nan", "na"}:
        return None
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            pass
    try:
        return float(value)
    except ValueError:
        return value


def read_metric_csv(path: Path) -> list[dict[str, Any]]:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"MetricFlow did not create a non-empty CSV: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise RuntimeError(f"MetricFlow CSV has no header: {path}")
        rows = [
            {
                str(key).strip().lower(): parse_csv_value(value or "")
                for key, value in row.items()
                if key is not None
            }
            for row in reader
        ]
    if not rows:
        raise RuntimeError(f"MetricFlow query returned no rows: {path.name}")
    return rows


def normalize_metric_rows(
    rows: list[dict[str, Any]], group_by: str | None
) -> list[dict[str, Any]]:
    """Give MetricFlow's short CSV dimension headers stable semantic names."""
    if not group_by:
        return rows
    if group_by.startswith("metric_time__"):
        for row in rows:
            value = row.get(group_by)
            if value is None:
                value = row.get("metric_time")
            if value is not None:
                year_text = str(value)[:4]
                if not re.fullmatch(r"\d{4}", year_text):
                    raise RuntimeError(
                        f"MetricFlow returned an unrecognized year value: {value!r}."
                    )
                row[group_by] = int(year_text)
        return rows

    short_name = group_by.rsplit("__", 1)[-1]
    for row in rows:
        if group_by not in row and short_name in row:
            row[group_by] = row[short_name]
    return rows


def validate_warehouse_identity(
    metadata: dict[str, str | None], trigger_run_id: str
) -> None:
    """Verify the serving publication and its actual upstream source run."""
    if metadata.get("warehouse_refresh_run_id") != trigger_run_id:
        raise RuntimeError(
            "warehouse-latest does not belong to the triggering Transformation run: "
            f"expected {trigger_run_id}, observed "
            f"{metadata.get('warehouse_refresh_run_id')!r}."
        )

    source_commit = metadata.get("warehouse_source_commit")
    if not source_commit:
        raise RuntimeError("warehouse-latest has no source commit identity.")

    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        raise RuntimeError("GITHUB_REPOSITORY is required for provenance validation.")

    mode1_run_id = metadata.get("warehouse_source_mode1_run_id")
    verified_run_id = mode1_run_id or trigger_run_id
    run = json.loads(
        subprocess.check_output(
            [
                "gh",
                "api",
                f"repos/{repository}/actions/runs/{verified_run_id}",
            ],
            text=True,
        )
    )

    expected_path = (
        ".github/workflows/mode1-incremental-loader.yml"
        if mode1_run_id
        else ".github/workflows/transformation-dbt-serving.yml"
    )
    expected_name = (
        "Loading SCD2 / Mode 1 Incremental Loader (Validated Staging)"
        if mode1_run_id
        else "Transformation / dbt Serving Refresh"
    )

    if (
        run.get("path") != expected_path
        or run.get("name") != expected_name
        or run.get("head_branch") != "main"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
    ):
        raise RuntimeError(
            f"Source run {verified_run_id} is not a successful completed "
            f"{expected_name} run on main."
        )

    if run.get("head_sha") != source_commit:
        raise RuntimeError(
            "warehouse-latest source commit does not match its verified upstream run: "
            f"expected {run.get('head_sha')!r}, "
            f"observed {source_commit!r}."
        )


def metric_query(
    name: str,
    metrics: str,
    group_by: str | None,
    output_dir: Path,
    env: dict[str, str],
) -> list[dict[str, Any]]:
    csv_path = output_dir / f"{name}.csv"
    csv_path.unlink(missing_ok=True)
    command = ["mf", "query", "--metrics", metrics]
    if group_by:
        command.extend(["--group-by", group_by])
    command.extend(["--csv", str(csv_path)])
    run(command, env)
    return normalize_metric_rows(read_metric_csv(csv_path), group_by)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--trigger-run-id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    database = args.database.expanduser().resolve()
    if not database.is_file():
        parser.error(f"DuckDB database does not exist: {database}")
    if args.trigger_run_id and not str(args.trigger_run_id).isdecimal():
        parser.error("--trigger-run-id must contain digits only")

    metadata = warehouse_refresh_metadata(database)
    if args.trigger_run_id:
        validate_warehouse_identity(metadata, str(args.trigger_run_id))
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = str(database)
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT)
    env["DBT_PROJECT_DIR"] = str(DBT_PROJECT)

    # Dependencies and parsing are isolated to this workflow's virtualenv.
    # The production dbt environment and dashboard requirements remain unchanged.
    run(["dbt", "deps", "--profiles-dir", str(DBT_PROJECT)], env)
    run(
        ["dbt", "run", "--select", "dim_date", "--profiles-dir", str(DBT_PROJECT)],
        env,
    )
    run(["dbt", "parse", "--profiles-dir", str(DBT_PROJECT)], env)
    run(["mf", "validate-configs"], env)
    run(["mf", "list", "metrics"], env)

    output_dir = args.output.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {
        "active_overall": metric_query(
            "active_overall", ACTIVE_METRICS, None, output_dir, env
        ),
        "active_by_permit_type": metric_query(
            "active_by_permit_type",
            ACTIVE_METRICS,
            "active_permit_revision__permit_type",
            output_dir,
            env,
        ),
        "active_by_year": metric_query(
            "active_by_year", ACTIVE_METRICS, "metric_time__year", output_dir, env
        ),
        "cleared_overall": metric_query(
            "cleared_overall", CLEARED_METRICS, None, output_dir, env
        ),
        "cleared_by_permit_type": metric_query(
            "cleared_by_permit_type",
            CLEARED_METRICS,
            "cleared_permit_revision__permit_type",
            output_dir,
            env,
        ),
        "cleared_by_year": metric_query(
            "cleared_by_year", CLEARED_METRICS, "metric_time__year", output_dir, env
        ),
    }

    payload = {
        "schema_version": 1,
        "backend": "duckdb",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "trigger_mode1_run_id": str(
            args.trigger_run_id or metadata["warehouse_refresh_run_id"]
        ),
        **metadata,
        **results,
    }
    output_file = output_dir / "toronto-metricflow-results.json"
    output_file.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Published MetricFlow result package: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
