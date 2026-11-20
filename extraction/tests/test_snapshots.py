import json
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from toronto_housing_extraction.config import load_config
from toronto_housing_extraction.extraction import SourceExtraction
from toronto_housing_extraction.snapshots import (
    build_provenance,
    build_source_manifest,
    create_snapshot_bundle,
    validate_release_provenance,
    validate_replay_contract,
)


def make_extraction(
    path: Path,
    source_name: str = "active_permits",
    row_count: int = 1,
) -> SourceExtraction:
    config = load_config(Path("config/sources.yml"))
    source = config.sources[source_name]

    frame = pd.DataFrame(
        {column: ["sample-value"] * row_count for column in source.required_columns}
    )

    frame["PERMIT_NUM"] = ["00123"] * row_count
    frame["REVISION_NUM"] = ["00"] * row_count
    frame["PERMIT_TYPE"] = ["Building"] * row_count

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)

    return SourceExtraction(
        source_name=source_name,
        extraction_started_at_utc="2026-11-15T00:00:00+00:00",
        extraction_completed_at_utc="2026-11-15T00:01:00+00:00",
        resource_id=f"{source_name}-resource",
        resource_name=source.title,
        extraction_method="datastore",
        fingerprint={"resource_id": f"{source_name}-resource"},
        row_count=row_count,
        quality_warnings=(),
        columns=tuple(frame.columns),
        parquet_path=path,
    )


def test_builds_valid_source_manifest(tmp_path):
    config = load_config(Path("config/sources.yml"))
    source = config.sources["active_permits"]

    extraction = make_extraction(tmp_path / "active.parquet")

    manifest = build_source_manifest(extraction, source)

    assert manifest["source_name"] == "active_permits"
    assert manifest["row_count"] == 1
    assert manifest["validation_status"] == "passed"
    assert manifest["parquet_size_bytes"] > 0
    assert len(manifest["parquet_sha256"]) == 64
    assert "PERMIT_NUM" in manifest["parquet_schema"]


def test_rejects_parquet_row_count_mismatch(tmp_path):
    config = load_config(Path("config/sources.yml"))
    source = config.sources["active_permits"]

    extraction = make_extraction(
        tmp_path / "active.parquet",
        row_count=2,
    )

    extraction = SourceExtraction(
        **{
            **extraction.__dict__,
            "row_count": 1,
        }
    )

    with pytest.raises(ValueError, match="rows"):
        build_source_manifest(extraction, source)


def test_creates_complete_snapshot_bundle(tmp_path):
    config = load_config(Path("config/sources.yml"))

    extractions = (
        make_extraction(
            tmp_path / "active.parquet",
            "active_permits",
        ),
        make_extraction(
            tmp_path / "cleared.parquet",
            "cleared_permits",
        ),
    )

    output = tmp_path / "snapshot.zip"

    bundle = create_snapshot_bundle(
        extractions,
        config,
        output,
        snapshot_id="test-snapshot-001",
    )

    assert bundle["format_version"] == 2
    assert bundle["complete"] is True
    assert bundle["validation_status"] == "passed"
    assert bundle["snapshot_id"] == "test-snapshot-001"
    assert "approved_for_warehouse" not in bundle

    with ZipFile(output) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == {
            "bundle.json",
            "active_permits/data.parquet",
            "active_permits/manifest.json",
            "cleared_permits/data.parquet",
            "cleared_permits/manifest.json",
        }
        archived_bundle = json.loads(archive.read("bundle.json"))
        assert archived_bundle["provenance"] == bundle["provenance"]
    assert bundle["extraction_started_at_utc"] == ("2026-11-15T00:00:00+00:00")
    assert bundle["extraction_completed_at_utc"] == ("2026-11-15T00:01:00+00:00")
    assert bundle["provenance"]["schema_version"] == 1
    assert bundle["provenance"]["producer"]["package_version"]


def test_refuses_incomplete_snapshot_pair(tmp_path):
    config = load_config(Path("config/sources.yml"))

    active = make_extraction(
        tmp_path / "active.parquet",
        "active_permits",
    )

    output = tmp_path / "snapshot.zip"

    with pytest.raises(ValueError, match="missing"):
        create_snapshot_bundle(
            (active,),
            config,
            output,
        )

    assert not output.exists()


def test_rejects_empty_snapshot_id(tmp_path):
    config = load_config(Path("config/sources.yml"))

    extractions = (
        make_extraction(tmp_path / "active.parquet", "active_permits"),
        make_extraction(tmp_path / "cleared.parquet", "cleared_permits"),
    )

    output = tmp_path / "snapshot.zip"

    with pytest.raises(ValueError, match="Snapshot ID must not be empty"):
        create_snapshot_bundle(
            extractions,
            config,
            output,
            snapshot_id="",
        )

    assert not output.exists()


def test_refuses_to_overwrite_existing_snapshot(tmp_path):
    config = load_config(Path("config/sources.yml"))

    extractions = (
        make_extraction(tmp_path / "active.parquet", "active_permits"),
        make_extraction(tmp_path / "cleared.parquet", "cleared_permits"),
    )

    output = tmp_path / "snapshot.zip"
    original_contents = b"existing snapshot must remain untouched"
    output.write_bytes(original_contents)

    with pytest.raises(FileExistsError, match="already exists"):
        create_snapshot_bundle(
            extractions,
            config,
            output,
            snapshot_id="test-snapshot-002",
        )

    assert output.read_bytes() == original_contents


def github_environment():
    return {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "push",
        "GITHUB_REPOSITORY": ("gagadanalyst/toronto-housing-bottleneck-analytics"),
        "GITHUB_WORKFLOW": "CI",
        "GITHUB_RUN_ID": "123456",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_SHA": "a" * 40,
    }


def test_builds_local_provenance():
    provenance = build_provenance({"GITHUB_ACTIONS": "false"})

    assert provenance["schema_version"] == 1
    assert provenance["producer"]["package_name"] == ("toronto-housing-extraction")
    assert provenance["producer"]["package_version"]
    assert provenance["producer"]["source_commit"] is None
    assert provenance["execution"]["provider"] == "local"
    assert provenance["execution"]["run_id"] is None

    with pytest.raises(ValueError, match="Only GitHub Actions"):
        validate_release_provenance(provenance)


def test_builds_github_actions_provenance():
    env = github_environment()
    env["SNAPSHOT_SOURCE_COMMIT"] = "b" * 40

    provenance = build_provenance(env)

    assert provenance["producer"]["source_commit"] == "b" * 40
    assert provenance["execution"]["provider"] == "github_actions"
    assert provenance["execution"]["repository"] == (
        "gagadanalyst/toronto-housing-bottleneck-analytics"
    )
    assert provenance["execution"]["run_id"] == "123456"
    assert provenance["execution"]["run_attempt"] == 2
    assert provenance["execution"]["run_url"] == (
        "https://github.com/gagadanalyst/"
        "toronto-housing-bottleneck-analytics/actions/runs/123456"
    )

    validate_release_provenance(provenance)


def test_uses_github_sha_for_normal_ci_run():
    provenance = build_provenance(github_environment())

    assert provenance["producer"]["source_commit"] == "a" * 40


def test_requires_explicit_source_commit_for_workflow_run():
    env = github_environment()
    env["GITHUB_EVENT_NAME"] = "workflow_run"

    with pytest.raises(ValueError, match="SNAPSHOT_SOURCE_COMMIT"):
        build_provenance(env)


def test_rejects_incomplete_github_execution_metadata():
    env = github_environment()
    env.pop("GITHUB_RUN_ATTEMPT")

    with pytest.raises(ValueError, match="GITHUB_RUN_ATTEMPT"):
        build_provenance(env)


def test_rejects_invalid_github_source_commit():
    env = github_environment()
    env["SNAPSHOT_SOURCE_COMMIT"] = "not-a-full-commit-sha"

    with pytest.raises(ValueError, match="full source commit SHA"):
        build_provenance(env)


def test_snapshot_bundle_contains_versioned_checksummed_replay_contract(
    tmp_path,
):
    config = load_config(Path("config/sources.yml"))
    extractions = (
        make_extraction(tmp_path / "active.parquet", "active_permits"),
        make_extraction(tmp_path / "cleared.parquet", "cleared_permits"),
    )
    output = tmp_path / "snapshot.zip"

    bundle = create_snapshot_bundle(extractions, config, output)
    contract = bundle["replay_contract"]

    assert contract["contract_id"] == "toronto-housing-snapshot-replay"
    assert contract["contract_version"] == 1
    assert contract["snapshot_format_version"] == 2
    assert contract["source_configuration"]["ckan"]["page_size"] == (
        config.ckan.page_size
    )
    assert contract["source_configuration"]["sources"]["active_permits"][
        "business_key"
    ] == ["PERMIT_NUM", "REVISION_NUM", "PERMIT_TYPE"]
    assert contract["normalization"]["field_types"]["PERMIT_NUM"] == "text"
    assert contract["replay_implementation"]["mode2_entrypoint"] == (
        "scripts/replay_snapshots.py"
    )
    assert contract["scd2_replay"]["observation_boundary"] == (
        "manifest.extraction_started_at_utc"
    )
    assert contract["scd2_replay"]["archive_order"] == [
        "bundle.extraction_started_at_utc",
        "run_id",
        "run_attempt",
        "snapshot_id",
    ]
    assert contract["scd2_replay"]["change_detection"] == "row_hash"
    assert contract["scd2_replay"]["validity_columns"] == {
        "from": "_dlt_valid_from",
        "to": "_dlt_valid_to",
    }
    assert len(bundle["replay_contract_sha256"]) == 64

    with ZipFile(output) as archive:
        archived = json.loads(archive.read("bundle.json"))
        assert archived["replay_contract"] == contract
        validate_replay_contract(
            archived["replay_contract"], archived["replay_contract_sha256"]
        )


def test_replay_contract_rejects_modified_content(tmp_path):
    config = load_config(Path("config/sources.yml"))
    extractions = (
        make_extraction(tmp_path / "active.parquet", "active_permits"),
        make_extraction(tmp_path / "cleared.parquet", "cleared_permits"),
    )
    bundle = create_snapshot_bundle(extractions, config, tmp_path / "snapshot.zip")
    altered = json.loads(json.dumps(bundle["replay_contract"]))
    altered["source_configuration"]["ckan"]["page_size"] += 1

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        validate_replay_contract(altered, bundle["replay_contract_sha256"])

    unsupported = dict(bundle["replay_contract"])
    unsupported["contract_version"] = 2
    with pytest.raises(ValueError, match="version is unsupported"):
        validate_replay_contract(unsupported, bundle["replay_contract_sha256"])
