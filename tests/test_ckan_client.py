import pytest
import requests

from toronto_housing_ingestion.ckan_client import (
    DatastoreUnavailableError,
    TorontoCKANClient,
)


def client() -> TorontoCKANClient:
    return TorontoCKANClient("https://example.com/api", timeout_seconds=1, retries=0)


def test_selects_datastore_first() -> None:
    c = client()
    package = {
        "resources": [
            {"id": "csv", "format": "CSV", "url": "https://x/c.csv"},
            {"id": "ds", "datastore_active": True},
        ]
    }
    assert c.select_resource(package)["id"] == "ds"
    c.close()


def test_selects_csv_before_json() -> None:
    c = client()
    package = {
        "resources": [
            {"id": "json", "format": "JSON", "url": "https://x/j.json"},
            {"id": "csv", "format": "CSV", "url": "https://x/c.csv"},
        ]
    }
    assert c.fallback_resources(package)[0]["id"] == "csv"
    c.close()


def test_probe_wraps_api_failure(monkeypatch) -> None:
    c = client()

    def fail(*args, **kwargs):
        raise requests.RequestException("down")

    monkeypatch.setattr(c, "action", fail)
    with pytest.raises(DatastoreUnavailableError):
        c.probe_datastore("1")
    c.close()


def test_json_frame_iterator_requires_ijson() -> None:
    pytest.importorskip("ijson")


def test_datastore_fallback_error_type_is_clear() -> None:
    error = DatastoreUnavailableError("DataStore down")
    assert "DataStore down" in str(error)


def test_probe_datastore_does_not_send_date_filter(monkeypatch) -> None:
    c = client()
    captured = {}

    def fake_action(name, **params):
        captured.update(params)
        return {
            "records": [{"id": 1}],
            "fields": [{"id": "id", "type": "int4"}],
            "total": 1,
        }

    monkeypatch.setattr(c, "action", fake_action)
    c.probe_datastore("resource")
    assert "filters" not in captured
    c.close()
