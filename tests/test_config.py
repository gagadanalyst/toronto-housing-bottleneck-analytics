from pathlib import Path

import yaml

from toronto_housing_ingestion.config import load_config, validate_config


def test_config_has_two_sources_and_warehouse_cutoff() -> None:
    raw = yaml.safe_load(Path("open_data_config.yml").read_text())
    validate_config(raw)

    expected_sources = {
        "active_permits": {
            "dataset_slug": "building-permits-active-permits",
            "warehouse_table": "raw_active_permits",
        },
        "cleared_permits": {
            "dataset_slug": "building-permits-cleared-permits",
            "warehouse_table": "raw_cleared_permits",
        },
    }

    assert set(raw["sources"]) == set(expected_sources)

    for name, expected in expected_sources.items():
        source = raw["sources"][name]
        assert source["dataset_slug"] == expected["dataset_slug"]
        assert source["warehouse_table"] == expected["warehouse_table"]
        assert source["warehouse_since"] == "2020-01-01"
        assert source["warehouse_date_column"] == "APPLICATION_DATE"
        assert source["warehouse_load"] is True
        assert source["warehouse_strategy"] == "scd2"
        assert source["primary_key"] == [
            "permit_num",
            "revision_num",
            "permit_type",
        ]
        assert "revision_type" not in source["primary_key"]
        assert "extract_since" not in source
        assert "extract_date_column" not in source


def test_config_rejects_incomplete_warehouse_filter() -> None:
    raw = {
        "ckan": {
            "base_url": "https://example.com/api",
            "timeout_seconds": 10,
            "retries": 1,
            "page_size": 100,
        },
        "warehouse": {
            "kind": "duckdb",
            "credentials": "data.db",
            "dataset_name": "raw",
        },
        "sources": {
            "x": {
                "title": "X",
                "dataset_slug": "x",
                "role": "core",
                "expected_cadence": "daily",
                "portal_url": "https://example.com",
                "required_columns": [],
                "pagination_sort": "_id asc",
                "warehouse_table": "raw_x",
                "warehouse_load": False,
                "primary_key": [],
                "warehouse_since": "2020-01-01",
            }
        },
    }
    try:
        validate_config(raw)
    except ValueError as exc:
        assert "warehouse_since" in str(exc)
    else:
        raise AssertionError("Expected validation failure")


def test_load_config_parses_project_yaml(monkeypatch) -> None:
    import toronto_housing_ingestion.config as config_module

    monkeypatch.setattr(config_module.sys, "version_info", (3, 14, 0))
    monkeypatch.setattr(config_module.sys, "version", "3.14.0")
    app = load_config(Path("open_data_config.yml"))
    assert len(app.sources) == 2
    assert app.sources[0].warehouse_since == "2020-01-01"


def test_replace_strategy_allows_empty_primary_key(monkeypatch, tmp_path):
    import toronto_housing_ingestion.config as config_module

    monkeypatch.setattr(config_module.sys, "version_info", (3, 14, 0))
    monkeypatch.setattr(config_module.sys, "version", "3.14.0")

    raw = yaml.safe_load(Path("open_data_config.yml").read_text())

    test_source = raw["sources"]["active_permits"].copy()
    test_source["title"] = "Synthetic replace test source"
    test_source["dataset_slug"] = "synthetic-replace-source"
    test_source["warehouse_table"] = "raw_synthetic_replace_source"
    test_source["warehouse_strategy"] = "replace"
    test_source["primary_key"] = []

    raw["sources"] = {"synthetic_replace_source": test_source}

    validate_config(raw)

    config_path = tmp_path / "test_config.yml"
    config_path.write_text(yaml.safe_dump(raw))

    app = load_config(config_path)
    source = next(s for s in app.sources if s.name == "synthetic_replace_source")

    assert source.warehouse_strategy == "replace"
    assert source.primary_key == ()
