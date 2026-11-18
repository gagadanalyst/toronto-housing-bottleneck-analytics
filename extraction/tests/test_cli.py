import logging
from types import SimpleNamespace

import toronto_housing_extraction.cli as cli


def test_main_creates_snapshot_and_cleans_staging(
    tmp_path, monkeypatch, capsys, caplog
):
    caplog.set_level(logging.INFO)
    output = tmp_path / "output"
    config = SimpleNamespace(
        ckan=SimpleNamespace(
            base_url="https://example.com/api",
            timeout_seconds=30,
            retries=2,
        ),
        sources={
            "active_permits": None,
            "cleared_permits": None,
        },
    )

    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "toronto-extract",
            "--config",
            str(tmp_path / "sources.yml"),
            "--output",
            str(output),
        ],
    )
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(
        cli.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="snapshot-test-001"),
    )

    client_state = {}

    class FakeClient:
        def __init__(self, **kwargs):
            client_state["options"] = kwargs

        def __enter__(self):
            client_state["entered"] = True
            return self

        def __exit__(self, *_args):
            client_state["closed"] = True

    monkeypatch.setattr(cli, "TorontoCKANClient", FakeClient)

    extractions = ("active", "cleared")
    extraction_calls = {}

    def fake_extract_sources(*, client, config, run_dir):
        extraction_calls.update(
            client=client,
            config=config,
            run_dir=run_dir,
        )
        return extractions

    monkeypatch.setattr(cli, "extract_sources", fake_extract_sources)

    bundle_calls = {}

    def fake_create_snapshot_bundle(results, config_arg, output_path, *, snapshot_id):
        bundle_calls.update(
            extractions=results,
            config=config_arg,
            output_path=output_path,
            snapshot_id=snapshot_id,
        )
        output_path.write_bytes(b"test archive")

        return {
            "snapshot_id": snapshot_id,
            "sources": {
                "active_permits": {"row_count": 120},
                "cleared_permits": {"row_count": 340},
            },
        }

    monkeypatch.setattr(
        cli,
        "create_snapshot_bundle",
        fake_create_snapshot_bundle,
    )

    result = cli.main()

    assert result == 0
    assert client_state["entered"] is True
    assert client_state["closed"] is True

    assert client_state["options"] == {
        "base_url": "https://example.com/api",
        "timeout_seconds": 30,
        "retries": 2,
    }

    run_dir = output / "staging" / "snapshot-test-001"
    bundle_path = output / "snapshots" / "snapshot-test-001.zip"

    assert extraction_calls["client"] is not None
    assert extraction_calls["config"] is config
    assert extraction_calls["run_dir"] == run_dir

    assert bundle_calls["extractions"] is extractions
    assert bundle_calls["config"] is config
    assert bundle_calls["output_path"] == bundle_path
    assert bundle_calls["snapshot_id"] == "snapshot-test-001"

    assert bundle_path.read_bytes() == b"test archive"
    assert not run_dir.exists()

    console_output = capsys.readouterr().out
    assert "Snapshot created successfully." in console_output
    assert "active_permits: 120 rows" in console_output
    assert "cleared_permits: 340 rows" in console_output

    assert "[1/4] Loading configuration..." in caplog.text
    assert "[2/4] Preparing Toronto CKAN client..." in caplog.text
    assert "[3/4] Extracting configured sources..." in caplog.text
    assert "[4/4] Packaging and verifying snapshot..." in caplog.text
    assert "Snapshot archive verified." in caplog.text
