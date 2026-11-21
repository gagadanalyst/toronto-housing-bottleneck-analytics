from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _runner_module():
    path = ROOT / "metricflow" / "run_metrics.py"
    spec = importlib.util.spec_from_file_location("toronto_metricflow_runner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_metricflow_dependencies_are_isolated_and_pinned():
    requirements = (ROOT / "metricflow" / "requirements.txt").read_text()
    assert requirements.strip() == "dbt-metricflow[dbt-duckdb]==0.16.0"
    assert "dbt-metricflow" not in (ROOT / "dbt" / "requirements.txt").read_text()
    assert (
        "metricflow"
        not in (ROOT / "dashboard" / "requirements.txt").read_text().lower()
    )


def test_existing_marts_define_the_metricflow_semantic_models():
    properties = yaml.safe_load(
        (ROOT / "dbt/models/marts/schema.yml").read_text(encoding="utf-8")
    )
    models = {model["name"]: model for model in properties["models"]}

    active = models["mart_active_permit_age"]
    assert active["semantic_model"]["enabled"] is True
    assert active["primary_entity"] == "active_permit_revision"
    assert active["agg_time_dimension"] == "application_date"
    active_metrics = {metric["name"] for metric in active["metrics"]}
    assert {
        "active_permit_revision_count",
        "eligible_active_permit_revision_count",
        "median_active_permit_age_days",
        "p90_active_permit_age_days",
    }.issubset(active_metrics)

    cleared = models["fct_cleared_permit_revision"]
    assert cleared["semantic_model"]["enabled"] is True
    assert cleared["primary_entity"] == "cleared_permit_revision"
    assert cleared["agg_time_dimension"] == "application_date"
    cleared_metrics = {metric["name"]: metric for metric in cleared["metrics"]}
    lifecycle_columns = {
        "median_application_to_issue_days": "application_to_issue_days",
        "p90_application_to_issue_days": "application_to_issue_days",
        "median_application_to_completion_days": "application_to_completion_days",
        "p90_application_to_completion_days": "application_to_completion_days",
        "median_issue_to_completion_days": "issue_to_completion_days",
        "p90_issue_to_completion_days": "issue_to_completion_days",
    }
    for metric_name, column in lifecycle_columns.items():
        metric = cleared_metrics[metric_name]
        expected_expr = (
            f"CASE WHEN LOWER(status) = 'closed' THEN {column} ELSE NULL END"
        )
        assert metric["expr"] == expected_expr
        assert "filter" not in metric


def test_existing_date_dimension_is_registered_as_a_daily_time_spine():
    properties = yaml.safe_load(
        (ROOT / "dbt/models/marts/schema.yml").read_text(encoding="utf-8")
    )
    models = {model["name"]: model for model in properties["models"]}
    assert models["dim_date"]["time_spine"]["standard_granularity_column"] == "date_day"
    assert models["dim_date"]["columns"][0]["granularity"] == "day"

    sql = (ROOT / "dbt/models/marts/dim_date.sql").read_text(encoding="utf-8")
    assert "fct_active_permit_revision" in sql
    assert "CURRENT_DATE" in sql


def test_metric_csv_reader_normalizes_headers_and_parses_numbers(tmp_path):
    module = _runner_module()
    path = tmp_path / "metric.csv"
    path.write_text(
        "METRIC_TIME__YEAR,ACTIVE_PERMIT_REVISION__PERMIT_TYPE,ACTIVE_PERMIT_REVISION_COUNT\n"
        "2025,Apartment,123\n",
        encoding="utf-8",
    )
    rows = module.read_metric_csv(path)
    assert rows == [
        {
            "metric_time__year": 2025,
            "active_permit_revision__permit_type": "Apartment",
            "active_permit_revision_count": 123,
        }
    ]
    assert module.parse_csv_value("NaN") is None
    assert module.parse_csv_value("NULL") is None

    short_columns = tmp_path / "metric-short-dimensions.csv"
    short_columns.write_text(
        "METRIC_TIME,PERMIT_TYPE,ACTIVE_PERMIT_REVISION_COUNT\n"
        "2025-01-01,Apartment,123\n",
        encoding="utf-8",
    )
    normalized = module.normalize_metric_rows(
        module.read_metric_csv(short_columns), "metric_time__year"
    )
    normalized = module.normalize_metric_rows(
        normalized, "active_permit_revision__permit_type"
    )
    assert normalized[0]["metric_time__year"] == 2025
    assert normalized[0]["active_permit_revision__permit_type"] == "Apartment"


def test_warehouse_result_must_match_triggering_mode1_run_and_commit():
    module = _runner_module()
    metadata = {
        "warehouse_refresh_run_id": "123456789",
        "warehouse_source_commit": "a" * 40,
    }
    module.validate_warehouse_identity(metadata, "123456789", "a" * 40)
    try:
        module.validate_warehouse_identity(metadata, "987654321", "a" * 40)
    except RuntimeError as exc:
        assert "does not belong" in str(exc)
    else:
        raise AssertionError("A stale warehouse run ID was accepted.")

    try:
        module.validate_warehouse_identity(metadata, "123456789", "b" * 40)
    except RuntimeError as exc:
        assert "source commit" in str(exc)
    else:
        raise AssertionError("A mismatched warehouse source commit was accepted.")


def test_separate_workflow_is_triggered_by_mode1_and_publishes_results():
    workflow = (ROOT / ".github/workflows/metricflow-metrics.yml").read_text()
    assert "pull_request:" in workflow
    assert "validate_pr:" in workflow
    assert "workflow_run:" in workflow
    assert "Mode 1 Incremental Loader (Validated Staging)" in workflow
    assert "Stage Mode 1 DuckDB changes" in workflow
    assert "source_run_head_sha" in workflow
    assert "ref: ${{ steps.source.outputs.source_run_head_sha }}" in workflow
    assert "--source-commit" in workflow
    assert "Publish the four verified DuckDB production assets" in workflow
    assert "metrics-latest" in workflow
    assert "mode1-incremental-loader.yml" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" not in workflow


def test_dashboard_exposes_the_published_metricflow_result_page():
    app = (ROOT / "dashboard/app.py").read_text(encoding="utf-8")
    assert '"Metrics & semantic layer"' in app
    assert "METRICFLOW_RELEASE_URL" in app
    assert "load_metricflow_results" in app
    assert "metricflow_page()" in app
    assert "warehouse_refresh_run_id" in app
    assert "Displays published MetricFlow results computed against the latest " in app
    assert 'page != "Metrics & semantic layer" and st.button(' in app
