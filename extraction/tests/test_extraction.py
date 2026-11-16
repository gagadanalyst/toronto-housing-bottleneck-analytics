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

        monkeypatch.setattr(
            client,
            "source_fingerprint",
            lambda *args: {"resource_id": "store-1"},
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
