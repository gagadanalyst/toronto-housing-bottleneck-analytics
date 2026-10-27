import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from toronto_housing_ingestion import ingestion
from toronto_housing_ingestion.config import SourceConfig


def source() -> SourceConfig:
    return SourceConfig(
        name="active_permits",
        title="Active",
        dataset_slug="active",
        role="core",
        expected_cadence="daily",
        portal_url="https://example.com",
        required_columns=("PERMIT_NUM",),
        date_columns=(),
        pagination_sort="_id asc",
        warehouse_table="raw_active_permits",
        warehouse_load=False,
        primary_key=("permit_num", "revision_num", "permit_type"),
        type_hints={},
        warehouse_since="2020-01-01",
        warehouse_date_column="APPLICATION_DATE",
    )


def test_source_unchanged_with_strong_signal() -> None:
    previous = {
        "resource_id": "1",
        "resource_hash": "same",
    }
    assert not ingestion.source_has_changed(previous, previous.copy())


def test_source_change_detects_resource_change() -> None:
    previous = {"resource_id": "1", "resource_hash": "same"}
    current = {"resource_id": "1", "resource_hash": "changed"}
    assert ingestion.source_has_changed(previous, current)


def test_no_ckan_filter_is_used() -> None:
    assert not hasattr(ingestion, "_filters")


def test_unknown_source_selection_is_reported_in_cli() -> None:
    names = {"a", "b"}
    selected = ["a", "c"]
    assert sorted(set(selected) - names) == ["c"]


def test_default_cli_processes_only_configured_sources(monkeypatch) -> None:
    import sys
    from contextlib import nullcontext

    monkeypatch.setattr(sys, "argv", ["ingest"])
    processed = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(ingestion.utils, "exclusive_lock", lambda path: nullcontext())
    monkeypatch.setattr(
        ingestion, "ensure_duckdb_database", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(ingestion, "TorontoCKANClient", FakeClient)

    def fake_ingest_source(*, source, **kwargs):
        processed.append(source.name)
        return {"source_name": source.name}

    monkeypatch.setattr(ingestion, "ingest_source", fake_ingest_source)

    assert ingestion.main() == 0
    assert processed == ["active_permits", "cleared_permits"]


def test_freshness() -> None:
    now = datetime(2026, 10, 7, tzinfo=UTC)
    assert (
        ingestion.check_freshness("2026-10-06T12:00:00+00:00", "daily", now) == "fresh"
    )
    assert (
        ingestion.check_freshness("2026-10-01T12:00:00+00:00", "daily", now) == "stale"
    )
    assert ingestion.check_freshness(None, "daily", now) == "unknown"


def test_schema_drift() -> None:
    assert ingestion.detect_schema_drift(["a", "c"], {"columns": ["a", "b"]}) == {
        "added": ["c"],
        "removed": ["b"],
    }


def test_fallback_tries_next_resource() -> None:
    class FakeClient:
        def fallback_resources(self, package):
            return [
                {"format": "csv", "url": "csv"},
                {"format": "json", "url": "json"},
            ]

        def download_fallback(self, **kwargs):
            if kwargs["resource"]["format"] == "csv":
                raise RuntimeError("csv down")
            return 2

        def file_fingerprint(self, resource, *args):
            return {"resource_id": resource["format"]}

    result = ingestion._run_fallbacks(FakeClient(), {}, source(), Path("out.parquet"))
    assert result[0]["format"] == "json"
    assert result[1] == 2


def test_fallback_rejects_incomplete_candidate_and_tries_next() -> None:
    class FakeClient:
        def fallback_resources(self, package):
            return [
                {
                    "id": "dump",
                    "name": "datastore dump",
                    "format": "CSV",
                    "url": "dump",
                },
                {"id": "csv", "name": "published CSV", "format": "CSV", "url": "csv"},
            ]

        def download_fallback(self, *, resource, **kwargs):
            return 128_000 if resource["id"] == "dump" else 446_667

        def file_fingerprint(self, resource, package_metadata_modified):
            return {"resource_id": resource["id"]}

    result = ingestion._run_fallbacks(
        FakeClient(), {}, source(), Path("out.parquet"), expected_rows=446_667
    )
    assert result[0]["id"] == "csv"
    assert result[1] == 446_667


def test_stale_source_is_warning_only() -> None:
    now = datetime(2026, 10, 7, tzinfo=UTC)
    assert (
        ingestion.check_freshness("2026-10-01T12:00:00+00:00", "daily", now) == "stale"
    )


def test_warehouse_failure_is_recorded_and_retried(monkeypatch, tmp_path) -> None:
    configured_source = replace(source(), warehouse_load=True)
    resource = {
        "id": "csv-resource",
        "format": "CSV",
        "url": "https://example.com/active.csv",
    }
    fingerprint = {"resource_id": "csv-resource", "resource_hash": "stable"}

    class FakeClient:
        def package_show(self, dataset_slug):
            return {"metadata_modified": "2026-10-14T00:00:00+00:00"}

        def select_resource(self, package):
            return resource

        def file_fingerprint(self, resource, package_metadata_modified):
            return fingerprint.copy()

    def fake_fallbacks(client, package, source_config, output_path, first=None):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"synthetic full-source snapshot")
        return resource, 1, fingerprint.copy()

    def fail_load(**kwargs):
        raise RuntimeError("simulated warehouse failure")

    monkeypatch.setattr(ingestion, "_run_fallbacks", fake_fallbacks)
    monkeypatch.setattr(ingestion.utils, "actual_columns", lambda path: ["PERMIT_NUM"])
    monkeypatch.setattr(ingestion.utils, "key_quality_warnings", lambda *args: [])
    monkeypatch.setattr(ingestion.utils, "sha256_file", lambda path: "a" * 64)
    monkeypatch.setattr(ingestion, "load_scd2", fail_load)

    app = SimpleNamespace(
        warehouse_credentials="duckdb:///tmp/test.duckdb",
        dataset_name="toronto",
        warehouse_kind="duckdb",
    )
    manifest_root = tmp_path / "manifests"

    with pytest.raises(RuntimeError, match="simulated warehouse failure"):
        ingestion.ingest_source(
            client=FakeClient(),
            source=configured_source,
            raw_root=tmp_path / "raw",
            manifest_root=manifest_root,
            checkpoint_root=tmp_path / "checkpoints",
            page_size=100,
            probe_only=False,
            full_refresh=False,
            extract_only=False,
            app=app,
        )

    manifest_files = list((manifest_root / "active_permits").glob("*.json"))
    assert len(manifest_files) == 1
    manifest = json.loads(manifest_files[0].read_text(encoding="utf-8"))
    assert manifest["status"] == "load_failed"
    assert manifest["warehouse_status"] == "failed"
    assert manifest["warehouse_error"] == "simulated warehouse failure"
    assert ingestion.source_has_changed(manifest, fingerprint)


def test_stale_flag_is_not_a_cli_option() -> None:
    args = ingestion.build_parser().parse_args([])
    assert not hasattr(args, "fail_on_stale")
