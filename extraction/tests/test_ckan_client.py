import pyarrow.parquet as pq

from toronto_housing_extraction.ckan_client import TorontoCKANClient


def test_prefers_datastore_over_csv():
    package = {
        "resources": [
            {"id": "csv-1", "format": "CSV", "url": "https://example.com/data.csv"},
            {"id": "store-1", "datastore_active": True},
        ]
    }

    with TorontoCKANClient("https://example.com") as client:
        resource = client.select_resource(package)

    assert resource["id"] == "store-1"


def test_fallback_resources_prefers_csv_over_json():
    package = {
        "resources": [
            {"id": "json-1", "format": "JSON", "url": "https://example.com/data.json"},
            {"id": "csv-1", "format": "CSV", "url": "https://example.com/data.csv"},
            {"id": "store-1", "datastore_active": True},
        ]
    }

    with TorontoCKANClient("https://example.com") as client:
        resources = client.fallback_resources(package)

    assert [resource["id"] for resource in resources] == [
        "csv-1",
        "json-1",
    ]


def test_datastore_extracts_and_combines_pages(tmp_path, monkeypatch):
    records = [
        {"_id": 1, "PERMIT_NUM": "001"},
        {"_id": 2, "PERMIT_NUM": "002"},
        {"_id": 3, "PERMIT_NUM": "003"},
    ]
    offsets = []

    def fake_action(action_name, **params):
        assert action_name == "datastore_search"
        offset = params["offset"]
        offsets.append(offset)

        return {
            "records": records[offset : offset + params["limit"]],
            "total": len(records),
        }

    output = tmp_path / "active.parquet"
    checkpoint_dir = tmp_path / "staging"

    with TorontoCKANClient("https://example.com") as client:
        monkeypatch.setattr(client, "action", fake_action)

        row_count = client.fetch_datastore(
            resource_id="resource-1",
            page_size=2,
            checkpoint_dir=checkpoint_dir,
            fingerprint={"resource_hash": "abc"},
            source_name="active_permits",
            output_path=output,
            pagination_sort="_id asc",
            required_columns=("PERMIT_NUM",),
        )

    table = pq.read_table(output)

    assert row_count == 3
    assert offsets == [0, 2]
    assert table.column("PERMIT_NUM").to_pylist() == ["001", "002", "003"]
    assert table.column("_id").to_pylist() == [1, 2, 3]
    assert not checkpoint_dir.exists()


class FakeResponse:
    """A small HTTP response substitute for fallback tests."""

    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.payload


def test_csv_fallback_preserves_identifiers(tmp_path, monkeypatch):
    payload = (
        b"PERMIT_NUM,REVISION_NUM,PERMIT_TYPE,APPLICATION_DATE\n"
        b"00123,00,Building,2024-01-15\n"
    )

    output = tmp_path / "active.parquet"

    with TorontoCKANClient("https://example.com") as client:
        monkeypatch.setattr(
            client.session,
            "get",
            lambda *args, **kwargs: FakeResponse(payload),
        )

        count = client.download_fallback(
            resource={
                "url": "https://example.com/data.csv",
                "format": "CSV",
            },
            output_path=output,
            source_name="active_permits",
            required_columns=("PERMIT_NUM", "REVISION_NUM"),
        )

    table = pq.read_table(output)

    assert count == 1
    assert table.column("PERMIT_NUM").to_pylist() == ["00123"]
    assert table.column("REVISION_NUM").to_pylist() == ["00"]


def test_json_fallback_preserves_identifiers(tmp_path, monkeypatch):
    payload = (
        b'{"result":{"records":[{'
        b'"PERMIT_NUM":"00123",'
        b'"REVISION_NUM":"00",'
        b'"PERMIT_TYPE":"Building",'
        b'"APPLICATION_DATE":"2024-01-15"'
        b"}]}}"
    )

    output = tmp_path / "active.parquet"

    with TorontoCKANClient("https://example.com") as client:
        monkeypatch.setattr(
            client.session,
            "get",
            lambda *args, **kwargs: FakeResponse(payload),
        )

        count = client.download_fallback(
            resource={
                "url": "https://example.com/data.json",
                "format": "JSON",
            },
            output_path=output,
            source_name="active_permits",
            required_columns=("PERMIT_NUM", "REVISION_NUM"),
        )

    table = pq.read_table(output)

    assert count == 1
    assert table.column("PERMIT_NUM").to_pylist() == ["00123"]
    assert table.column("REVISION_NUM").to_pylist() == ["00"]


def test_datastore_fingerprint_includes_cache_marker_and_total_estimation():
    with TorontoCKANClient("https://example.com") as client:
        fingerprint = client.source_fingerprint(
            {
                "id": "resource-1",
                "datastore_cache_last_update": "2026-11-16T10:23:21.445419",
                "last_modified": None,
            },
            {
                "declared_total": 123,
                "total_was_estimated": False,
                "schema_sha256": "a" * 64,
            },
            "package-metadata-timestamp",
        )

    assert fingerprint["fingerprint_version"] == 1
    assert fingerprint["datastore_cache_last_update"] == ("2026-11-16T10:23:21.445419")
    assert fingerprint["datastore_total"] == 123
    assert fingerprint["total_was_estimated"] is False


def test_estimated_datastore_total_paginates_until_empty_page(tmp_path, monkeypatch):
    records = [
        {"_id": 1, "PERMIT_NUM": "001"},
        {"_id": 2, "PERMIT_NUM": "002"},
        {"_id": 3, "PERMIT_NUM": "003"},
    ]
    offsets = []

    def fake_action(action_name, **params):
        assert action_name == "datastore_search"
        offset = params["offset"]
        offsets.append(offset)
        return {
            "records": records[offset : offset + params["limit"]],
            "total": 1,  # Deliberately lower than the real row count.
            "total_was_estimated": True,
        }

    output = tmp_path / "active.parquet"
    checkpoint_dir = tmp_path / "staging"

    with TorontoCKANClient("https://example.com") as client:
        monkeypatch.setattr(client, "action", fake_action)
        row_count = client.fetch_datastore(
            resource_id="resource-1",
            page_size=2,
            checkpoint_dir=checkpoint_dir,
            fingerprint={"resource_hash": "abc", "total_was_estimated": True},
            source_name="active_permits",
            output_path=output,
            pagination_sort="_id asc",
            required_columns=("PERMIT_NUM",),
            total_was_estimated=True,
        )

    table = pq.read_table(output)
    assert row_count == 3
    assert offsets == [0, 2, 3]
    assert table.column("PERMIT_NUM").to_pylist() == ["001", "002", "003"]
    assert not checkpoint_dir.exists()
