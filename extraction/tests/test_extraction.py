from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from toronto_housing_extraction.ckan_client import (
    DatastoreUnavailableError,
    TorontoCKANClient,
)
from toronto_housing_extraction.config import load_config
from toronto_housing_extraction.extraction import (
    SourceExtraction,
    extract_source,
    extract_sources,
)
from toronto_housing_extraction.schema import normalize_source_schema


def write_sample_parquet(path, source):
    values = {
        column: ["2024-01-15" if column.endswith("_DATE") else f"sample-{column}"]
        for column in source.required_columns
    }

    values.update(
        {
            "PERMIT_NUM": ["00123"],
            "REVISION_NUM": ["00"],
            "PERMIT_TYPE": ["Building"],
        }
    )

    frame = normalize_source_schema(pd.DataFrame(values))
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_extracts_source_using_datastore(tmp_path, monkeypatch):
    config = load_config(Path("config/sources.yml"))
    ckan = config.ckan
    source = config.sources["active_permits"]

    package = {
        "metadata_modified": "2026-11-15T10:00:00",
        "resources": [{"id": "store-1", "name": "Active", "datastore_active": True}],
    }

    with TorontoCKANClient(ckan.base_url) as client:
        monkeypatch.setattr(client, "package_show", lambda slug: package)

        monkeypatch.setattr(
            client,
            "probe_datastore",
            lambda resource_id: {
                "declared_total": 1,
                "field_names": list(source.required_columns),
            },
        )

        marker = "2026-11-15T10:00:00.000000"
        monkeypatch.setattr(
            client,
            "action",
            lambda action_name, **params: {
                "datastore_cache_last_update": marker,
                "last_modified": None,
            },
        )
        monkeypatch.setattr(
            client,
            "source_fingerprint",
            lambda *args: {
                "fingerprint_version": 1,
                "resource_id": "store-1",
                "datastore_cache_last_update": marker,
                "resource_last_modified": None,
            },
        )

        def fake_fetch(**kwargs):
            write_sample_parquet(kwargs["output_path"], source)
            return 1

        monkeypatch.setattr(client, "fetch_datastore", fake_fetch)

        result = extract_source(
            client=client,
            source=source,
            ckan=ckan,
            run_dir=tmp_path,
        )

    assert result.extraction_method == "datastore"
    assert result.fingerprint_reliable is True
    assert result.row_count == 1
    assert result.parquet_path.is_file()
    assert result.quality_warnings == ("below_minimum_row_count",)


def test_falls_back_to_csv_when_datastore_probe_fails(tmp_path, monkeypatch):
    config = load_config(Path("config/sources.yml"))
    ckan = config.ckan
    source = config.sources["active_permits"]

    package = {
        "metadata_modified": "2026-11-15T10:00:00",
        "resources": [
            {"id": "store-1", "datastore_active": True},
            {
                "id": "csv-1",
                "name": "Active CSV",
                "format": "CSV",
                "url": "https://example.com/active.csv",
            },
        ],
    }

    downloaded_resources = []

    with TorontoCKANClient(ckan.base_url) as client:
        monkeypatch.setattr(client, "package_show", lambda slug: package)

        def failed_probe(resource_id):
            raise DatastoreUnavailableError("DataStore unavailable")

        monkeypatch.setattr(client, "probe_datastore", failed_probe)

        def fake_download(**kwargs):
            downloaded_resources.append(kwargs["resource"]["id"])
            write_sample_parquet(kwargs["output_path"], source)
            return 1

        monkeypatch.setattr(client, "download_fallback", fake_download)
        monkeypatch.setattr(
            client,
            "file_fingerprint",
            lambda *args: {"resource_id": "csv-1"},
        )

        result = extract_source(
            client=client,
            source=source,
            ckan=ckan,
            run_dir=tmp_path,
        )

    assert result.extraction_method == "file_fallback"
    assert downloaded_resources == ["csv-1"]
    assert result.row_count == 1
    assert result.parquet_path.is_file()


def _source_result(source_name):
    return SourceExtraction(
        source_name=source_name,
        extraction_started_at_utc="2026-11-15T00:00:00+00:00",
        extraction_completed_at_utc="2026-11-15T00:01:00+00:00",
        resource_id=f"{source_name}-resource",
        resource_name=source_name,
        extraction_method="datastore",
        fingerprint={},
        row_count=1,
        quality_warnings=(),
        columns=("PERMIT_NUM",),
        parquet_path=Path(f"{source_name}.parquet"),
    )


def test_extracts_both_sources_as_one_pair(tmp_path, monkeypatch):
    config = load_config(Path("config/sources.yml"))
    extracted = []

    def fake_extract_source(**kwargs):
        name = kwargs["source"].name
        extracted.append(name)
        return _source_result(name)

    monkeypatch.setattr(
        "toronto_housing_extraction.extraction.extract_source",
        fake_extract_source,
    )

    results = extract_sources(
        client=object(),
        config=config,
        run_dir=tmp_path,
    )

    assert extracted == ["active_permits", "cleared_permits"]
    assert len(results) == 2


def test_pair_fails_when_one_source_extraction_fails(tmp_path, monkeypatch):
    config = load_config(Path("config/sources.yml"))
    attempted = []

    def fake_extract_source(**kwargs):
        name = kwargs["source"].name
        attempted.append(name)

        if name == "active_permits":
            raise ValueError("Invalid Parquet data")

        return _source_result(name)

    monkeypatch.setattr(
        "toronto_housing_extraction.extraction.extract_source",
        fake_extract_source,
    )

    with pytest.raises(RuntimeError, match="Snapshot extraction incomplete"):
        extract_sources(
            client=object(),
            config=config,
            run_dir=tmp_path,
        )

    assert attempted == ["active_permits", "cleared_permits"]


def test_discards_datastore_snapshot_if_source_marker_changes_during_extract(
    tmp_path, monkeypatch
):
    config = load_config(Path("config/sources.yml"))
    source = config.sources["active_permits"]
    package = {
        "metadata_modified": "2026-11-15T10:00:00",
        "resources": [{"id": "store-1", "name": "Active", "datastore_active": True}],
    }
    markers = iter(["2026-11-15T10:00:00.000000", "2026-11-15T10:05:00.000000"])

    with TorontoCKANClient(config.ckan.base_url) as client:
        monkeypatch.setattr(client, "package_show", lambda slug: package)
        monkeypatch.setattr(
            client,
            "probe_datastore",
            lambda resource_id: {
                "declared_total": 1,
                "field_names": list(source.required_columns),
                "total_was_estimated": False,
            },
        )
        monkeypatch.setattr(
            client,
            "action",
            lambda action_name, **params: {
                "datastore_cache_last_update": next(markers),
                "last_modified": None,
            },
        )
        monkeypatch.setattr(
            client,
            "source_fingerprint",
            lambda *args: {
                "fingerprint_version": 1,
                "resource_id": "store-1",
                "datastore_cache_last_update": "2026-11-15T10:00:00.000000",
                "resource_last_modified": None,
            },
        )

        def fake_fetch(**kwargs):
            write_sample_parquet(kwargs["output_path"], source)
            return 1

        monkeypatch.setattr(client, "fetch_datastore", fake_fetch)

        with pytest.raises(RuntimeError, match="source changed during extraction"):
            extract_source(
                client=client,
                source=source,
                ckan=config.ckan,
                run_dir=tmp_path,
            )


def _reliable_source_result(source_name):
    marker = "2026-11-15T10:00:00.000000"
    return replace(
        _source_result(source_name),
        fingerprint={
            "datastore_cache_last_update": marker,
            "resource_last_modified": None,
        },
        fingerprint_reliable=True,
    )


def test_rechecks_both_source_markers_after_pair_completes(tmp_path, monkeypatch):
    config = load_config(Path("config/sources.yml"))
    extracted = []
    checked = []
    marker = "2026-11-15T10:00:00.000000"

    def fake_extract_source(**kwargs):
        name = kwargs["source"].name
        extracted.append(name)
        return _reliable_source_result(name)

    monkeypatch.setattr(
        "toronto_housing_extraction.extraction.extract_source",
        fake_extract_source,
    )

    with TorontoCKANClient(config.ckan.base_url) as client:

        def fake_action(action_name, **params):
            assert action_name == "resource_show"
            checked.append(params["id"])
            return {"datastore_cache_last_update": marker, "last_modified": None}

        monkeypatch.setattr(client, "action", fake_action)
        results = extract_sources(client=client, config=config, run_dir=tmp_path)

    assert len(results) == 2
    assert extracted == ["active_permits", "cleared_permits"]
    assert checked == ["active_permits-resource", "cleared_permits-resource"]


def test_pair_fails_when_first_source_changes_during_second_extraction(
    tmp_path, monkeypatch
):
    config = load_config(Path("config/sources.yml"))
    current_active_marker = "2026-11-15T10:00:00.000000"

    def fake_extract_source(**kwargs):
        nonlocal current_active_marker
        name = kwargs["source"].name
        if name == "cleared_permits":
            # Simulate Active changing after its own extraction check but while
            # Cleared is still being extracted.
            current_active_marker = "2026-11-15T10:05:00.000000"
        return _reliable_source_result(name)

    monkeypatch.setattr(
        "toronto_housing_extraction.extraction.extract_source",
        fake_extract_source,
    )

    with TorontoCKANClient(config.ckan.base_url) as client:

        def fake_action(action_name, **params):
            assert action_name == "resource_show"
            marker = (
                current_active_marker
                if params["id"] == "active_permits-resource"
                else "2026-11-15T10:00:00.000000"
            )
            return {"datastore_cache_last_update": marker, "last_modified": None}

        monkeypatch.setattr(client, "action", fake_action)
        with pytest.raises(RuntimeError, match="changed during paired extraction"):
            extract_sources(client=client, config=config, run_dir=tmp_path)
