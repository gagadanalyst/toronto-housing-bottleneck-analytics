from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts import mode1_state
from scripts.replay_ledger import ReplayLedger


def _mode2_fixture(
    tmp_path: Path,
    *,
    backend: str = "duckdb",
    history_scope: str = "full",
    terminal_status: str = "SUCCEEDED",
):
    ledger_path = tmp_path / f"{backend}-mode2-ledger.sqlite"
    ledger = ReplayLedger(ledger_path)
    candidate = (
        "data/recovery/snapshot-replay/duckdb/toronto-replay-12345.duckdb"
        if backend == "duckdb"
        else "TORONTO.TORONTO_SNAPSHOT_REPLAY_12345"
    )
    with sqlite3.connect(ledger_path) as connection:
        for index in range(2):
            snapshot_id = f"snapshot-{index + 1}"
            for source_name in ("active_permits", "cleared_permits"):
                connection.execute(
                    """INSERT INTO replay_loads (
                           backend, source_name, snapshot_id, format_version,
                           order_timestamp, run_id, run_attempt, source_commit,
                           archive_sha256, release_tag, asset_name, status,
                           error, updated_at_utc
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        backend,
                        source_name,
                        snapshot_id,
                        2,
                        float(index + 1),
                        100 + index,
                        1,
                        "a" * 40,
                        "b" * 64,
                        "toronto-raw-snapshot-v2-archive-2026-11",
                        f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
                        "succeeded",
                        None,
                        "2026-11-18T00:00:00+00:00",
                    ),
                )

    ledger.start_generation(
        backend,
        "12345:1",
        mode="rebuild",
        history_scope=history_scope,
        selected_snapshot_count=2,
        candidate=candidate,
    )
    if terminal_status != "RUNNING":
        ledger.finish_generation(backend, "12345:1", terminal_status)

    candidate_file = None
    if backend == "duckdb":
        candidate_file = tmp_path / "toronto-replay-12345.duckdb"
        candidate_file.write_bytes(b"mode2-duckdb-candidate-bytes")
    return ledger_path, candidate_file, candidate


def _passed_summary(candidate: str, backend: str = "duckdb") -> dict:
    return {
        "backend": backend,
        "mode": "incremental",
        "status": "passed",
        "candidate": candidate,
        "sources": {
            "active_permits": {"status": "passed"},
            "cleared_permits": {"status": "passed"},
        },
        "history_validation": {"status": "passed"},
    }


def test_mode2_metadata_requires_successful_full_history_and_two_sources(tmp_path):
    ledger_path, _, _ = _mode2_fixture(tmp_path)
    result = mode1_state._mode2_generation(ledger_path, "duckdb", "12345:1")
    assert result["baseline_snapshot_id"] == "snapshot-2"
    assert result["baseline_snapshot_count"] == 2
    assert result["selected_snapshot_count"] == 2


@pytest.mark.parametrize("backend", ("duckdb", "snowflake"))
def test_mode2_metadata_accepts_source_specific_observation_timestamps(
    tmp_path, backend
):
    ledger_path, _, _ = _mode2_fixture(tmp_path, backend=backend)
    with sqlite3.connect(ledger_path) as connection:
        connection.execute(
            """UPDATE replay_loads
               SET order_timestamp = order_timestamp + 0.5
               WHERE backend = ? AND source_name = 'cleared_permits'""",
            (backend,),
        )

    result = mode1_state._mode2_generation(ledger_path, backend, "12345:1")

    assert result["baseline_snapshot_id"] == "snapshot-2"
    assert result["baseline_snapshot_count"] == 2
    assert result["selected_snapshot_count"] == 2


@pytest.mark.parametrize("backend", ("duckdb", "snowflake"))
def test_mode2_metadata_rejects_mismatched_source_archive_identity(tmp_path, backend):
    ledger_path, _, _ = _mode2_fixture(tmp_path, backend=backend)
    with sqlite3.connect(ledger_path) as connection:
        connection.execute(
            """UPDATE replay_loads
               SET archive_sha256 = ?
               WHERE backend = ? AND snapshot_id = 'snapshot-2'
                 AND source_name = 'cleared_permits'""",
            ("c" * 64, backend),
        )

    with pytest.raises(RuntimeError, match="duplicated or inconsistent"):
        mode1_state._mode2_generation(ledger_path, backend, "12345:1")


@pytest.mark.parametrize(
    ("history_scope", "terminal_status", "message"),
    [
        ("partial", "SUCCEEDED", "full-history"),
        ("full", "FAILED", "not SUCCEEDED"),
        ("full", "RUNNING", "not SUCCEEDED"),
    ],
)
def test_mode2_metadata_rejects_unaccepted_generation(
    tmp_path, history_scope, terminal_status, message
):
    ledger_path, _, _ = _mode2_fixture(
        tmp_path,
        history_scope=history_scope,
        terminal_status=terminal_status,
    )
    with pytest.raises(RuntimeError, match=message):
        mode1_state._mode2_generation(ledger_path, "duckdb", "12345:1")


def test_mode1_duckdb_bootstrap_copies_candidate_and_sqlite_ledger(tmp_path):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"

    result = mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )

    assert (
        working_dir / "candidate.duckdb"
    ).read_bytes() == candidate_file.read_bytes()
    assert result["candidate_sha256"] == mode1_state._sha256(
        working_dir / "candidate.duckdb"
    )
    assert result["baseline_snapshot_id"] == "snapshot-2"
    assert result["baseline_snapshot_count"] == 2
    marker = ReplayLedger(working_dir / "replay-ledger.sqlite").bootstrap_record(
        "duckdb"
    )
    assert marker["generation_id"] == "12345:1"
    assert not ReplayLedger(ledger_path).bootstrap_record("duckdb")


def test_mode1_bootstrap_rejects_candidate_name_mismatch(tmp_path):
    ledger_path, _, _ = _mode2_fixture(tmp_path)
    candidate_file = tmp_path / "different-name.duckdb"
    candidate_file.write_bytes(b"candidate")

    with pytest.raises(RuntimeError, match="candidate name"):
        mode1_state.bootstrap_duckdb_working_state(
            mode2_ledger_path=ledger_path,
            mode2_candidate_path=candidate_file,
            working_dir=tmp_path / "working",
            generation_id="12345:1",
        )

    assert not (tmp_path / "working").exists()


def test_mode1_state_bundle_round_trips_candidate_and_working_ledger(tmp_path):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"
    mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(_passed_summary(str(working_dir / "candidate.duckdb"))),
        encoding="utf-8",
    )

    bundle, checksum, manifest = mode1_state.create_state_bundle(
        backend="duckdb",
        working_dir=working_dir,
        summary_path=summary_path,
        run_id="500",
        run_attempt="1",
    )

    restored_dir = tmp_path / "restored"
    restored = mode1_state.restore_state_bundle(bundle, restored_dir)
    assert mode1_state._sha256(bundle) == checksum
    assert restored["generation_id"] == "12345:1"
    assert restored["last_successful_run_id"] == "500"
    assert (
        restored_dir / "candidate.duckdb"
    ).read_bytes() == candidate_file.read_bytes()
    marker = ReplayLedger(restored_dir / "replay-ledger.sqlite").bootstrap_record(
        "duckdb"
    )
    assert marker["generation_id"] == "12345:1"
    assert manifest["candidate_sha256"] == mode1_state._sha256(
        restored_dir / "candidate.duckdb"
    )


def test_mode1_state_bundle_rejects_failed_incremental_summary(tmp_path):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"
    mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )
    summary = _passed_summary(str(working_dir / "candidate.duckdb"))
    summary["history_validation"]["status"] = "failed"
    summary["status"] = "failed"
    summary["sources"]["active_permits"]["status"] = "failed"
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    with pytest.raises(RuntimeError, match="candidate SCD2 validation passed"):
        mode1_state.create_state_bundle(
            backend="duckdb",
            working_dir=working_dir,
            summary_path=summary_path,
            run_id="501",
            run_attempt="1",
        )


def test_mode1_state_bundle_preserves_one_successful_stream_for_retry(tmp_path):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"
    mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )
    summary = _passed_summary(str(working_dir / "candidate.duckdb"))
    summary["status"] = "failed"
    summary["sources"]["active_permits"]["status"] = "failed"
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    _bundle, _checksum, manifest = mode1_state.create_state_bundle(
        backend="duckdb",
        working_dir=working_dir,
        summary_path=summary_path,
        run_id="502",
        run_attempt="1",
    )
    assert manifest["handoff_status"] == "retryable"
    assert manifest["last_attempt_run_id"] == "502"
    assert "last_successful_run_id" not in manifest


def test_release_restore_falls_back_when_backend_has_no_pointer(tmp_path, monkeypatch):
    from types import SimpleNamespace

    calls = []

    def fake_run(command, *, check=True):
        calls.append(command)
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"assets": []}),
            stderr="",
        )

    monkeypatch.setattr(mode1_state, "_run", fake_run)

    result = mode1_state._restore_latest_release_state(
        repository="owner/repo",
        backend="snowflake",
        working_dir=tmp_path / "working",
    )

    assert result is None
    assert len(calls) == 1
    assert not (tmp_path / "working").exists()


def test_latest_release_pointer_uses_run_id_then_attempt():
    assets = [
        {"name": "toronto-mode1-state-duckdb-499-9.json"},
        {"name": "toronto-mode1-state-duckdb-499-9.zip"},
        {"name": "toronto-mode1-state-duckdb-500-1.json"},
        {"name": "toronto-mode1-state-duckdb-500-1.zip"},
        {"name": "toronto-mode1-state-duckdb-500-2.json"},
        {"name": "toronto-mode1-state-duckdb-500-2.zip"},
        {"name": "toronto-mode1-state-snowflake-900-1.json"},
        {"name": "toronto-mode1-state-snowflake-900-1.zip"},
        {"name": "toronto-mode1-state-duckdb-latest.json"},
    ]

    assert mode1_state._latest_release_pointer(assets, "duckdb") == (
        "toronto-mode1-state-duckdb-500-2.json"
    )
    assert mode1_state._latest_release_pointer(assets, "snowflake") == (
        "toronto-mode1-state-snowflake-900-1.json"
    )


def test_release_restore_does_not_hide_release_access_errors(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(
        mode1_state,
        "_run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="connection reset"
        ),
    )

    with pytest.raises(
        RuntimeError, match="Unable to inspect Mode 1 state Release assets"
    ):
        mode1_state._restore_latest_release_state(
            repository="owner/repo",
            backend="duckdb",
            working_dir=tmp_path / "working",
        )


def test_mode1_snowflake_schema_cloning_uses_run_scoped_names():
    from scripts.replay_targets import INCREMENTAL_SNOWFLAKE_SCHEMA_RE

    assert INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch("RAW_STAGE_12345_2")
    assert not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch("TORONTO.RAW")
    assert not INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch("RAW_STAGE_abc_2")


def test_mode1_initial_baseline_checkpoint_round_trips_without_incremental_summary(
    tmp_path,
):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"
    mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )
    manifest_path = working_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update({"prepared_run_id": "900", "prepared_run_attempt": "1"})
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    bundle, checksum, created = mode1_state.create_state_bundle(
        backend="duckdb",
        working_dir=working_dir,
        summary_path=None,
        run_id="900",
        run_attempt="1",
        bootstrap_only=True,
    )

    restored_dir = tmp_path / "restored-baseline"
    restored = mode1_state.restore_state_bundle(bundle, restored_dir)
    assert mode1_state._sha256(bundle) == checksum
    assert created["handoff_status"] == "baseline"
    assert restored["handoff_status"] == "baseline"
    assert restored["generation_id"] == "12345:1"
    assert (
        restored_dir / "candidate.duckdb"
    ).read_bytes() == candidate_file.read_bytes()
    summary = json.loads((restored_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["mode"] == "bootstrap"
    assert summary["baseline_generation_id"] == "12345:1"


def test_mode1_initial_baseline_checkpoint_rejects_changed_duckdb_candidate(tmp_path):
    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    working_dir = tmp_path / "working"
    mode1_state.bootstrap_duckdb_working_state(
        mode2_ledger_path=ledger_path,
        mode2_candidate_path=candidate_file,
        working_dir=working_dir,
        generation_id="12345:1",
    )
    (working_dir / "candidate.duckdb").write_bytes(b"unexpected change")

    with pytest.raises(RuntimeError, match="candidate changed"):
        mode1_state.create_state_bundle(
            backend="duckdb",
            working_dir=working_dir,
            summary_path=None,
            run_id="901",
            run_attempt="1",
            bootstrap_only=True,
        )


def test_latest_release_pointer_requires_its_bundle_asset():
    assets = [
        {"name": "toronto-mode1-state-duckdb-499-9.json"},
        {"name": "toronto-mode1-state-duckdb-500-1.json"},
        {"name": "toronto-mode1-state-duckdb-500-1.zip"},
        {"name": "toronto-mode1-state-duckdb-500-2.json"},
        {"name": "toronto-mode1-state-duckdb-500-2.zip"},
    ]

    assert mode1_state._latest_release_pointer(assets, "duckdb") == (
        "toronto-mode1-state-duckdb-500-2.json"
    )


def test_release_cleanup_retains_latest_two_backend_checkpoints(tmp_path, monkeypatch):
    from types import SimpleNamespace

    assets = [
        {"name": f"toronto-mode1-state-duckdb-{run_id}-1.{extension}"}
        for run_id in (100, 200, 300)
        for extension in ("zip", "json")
    ] + [
        {"name": "toronto-mode1-state-snowflake-999-1.zip"},
        {"name": "toronto-mode1-state-snowflake-999-1.json"},
    ]
    deleted = []
    monkeypatch.setattr(mode1_state, "_release_assets", lambda _repo: assets)

    def fake_run(command, *, check=True):
        deleted.append(command[4])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(mode1_state, "_run", fake_run)
    mode1_state._cleanup_old_release_assets(
        repository="owner/repo",
        backend="duckdb",
        keep_bundle="toronto-mode1-state-duckdb-300-1.zip",
        keep_pointer="toronto-mode1-state-duckdb-300-1.json",
    )

    assert set(deleted) == {
        "toronto-mode1-state-duckdb-100-1.zip",
        "toronto-mode1-state-duckdb-100-1.json",
    }
    assert all("snowflake" not in name for name in deleted)


def test_prepare_state_marks_first_bootstrap_for_checkpoint(tmp_path, monkeypatch):
    import tarfile
    from argparse import Namespace

    ledger_path, candidate_file, _ = _mode2_fixture(tmp_path)
    archive_path = tmp_path / "mode2-state.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(ledger_path, arcname="duckdb-ledger.sqlite")
        archive.add(candidate_file, arcname="duckdb/toronto-replay-12345.duckdb")

    def unexpected_state_restore(**_kwargs):
        pytest.fail("Explicit Mode 2 rebootstrap must bypass existing Release state.")

    monkeypatch.setattr(
        mode1_state, "_restore_latest_release_state", unexpected_state_restore
    )
    monkeypatch.setattr(
        mode1_state,
        "_download_mode2_artifact",
        lambda **_kwargs: archive_path,
    )
    outputs = []
    monkeypatch.setattr(mode1_state, "_write_github_outputs", outputs.append)

    result = mode1_state.prepare_state(
        Namespace(
            backend="duckdb",
            repository="owner/repo",
            working_dir=tmp_path / "working",
            run_id="900",
            run_attempt="1",
            baseline_run_id="12345",
            baseline_attempt="1",
            rebootstrap_from_mode2=True,
            snowflake_credentials="",
        )
    )

    assert result["needs_initial_checkpoint"] == "true"
    assert outputs[0]["needs_initial_checkpoint"] == "true"
    assert (
        tmp_path / "working" / "candidate.duckdb"
    ).read_bytes() == candidate_file.read_bytes()


def test_prepare_state_requires_mode2_run_for_explicit_rebootstrap(
    tmp_path, monkeypatch
):
    from argparse import Namespace

    def unexpected_state_restore(**_kwargs):
        pytest.fail("A missing Mode 2 run ID must be rejected before Release restore.")

    monkeypatch.setattr(
        mode1_state, "_restore_latest_release_state", unexpected_state_restore
    )
    args = Namespace(
        backend="duckdb",
        repository="owner/repo",
        working_dir=tmp_path / "working",
        run_id="901",
        run_attempt="1",
        baseline_run_id="",
        baseline_attempt="1",
        rebootstrap_from_mode2=True,
        snowflake_credentials="",
    )

    with pytest.raises(ValueError, match="requires baseline_run_id"):
        mode1_state.prepare_state(args)


def test_prepare_state_rejects_stale_release_baseline_without_explicit_rebootstrap(
    tmp_path, monkeypatch
):
    from argparse import Namespace

    monkeypatch.setattr(
        mode1_state,
        "_restore_latest_release_state",
        lambda **_kwargs: {
            "backend": "duckdb",
            "generation_id": "44502575929:1",
        },
    )
    args = Namespace(
        backend="duckdb",
        repository="owner/repo",
        working_dir=tmp_path / "working",
        run_id="902",
        run_attempt="1",
        baseline_run_id="44650647432",
        baseline_attempt="1",
        rebootstrap_from_mode2=False,
        snowflake_credentials="",
    )

    with pytest.raises(RuntimeError, match="rebootstrap_from_mode2=true"):
        mode1_state.prepare_state(args)


def test_snowflake_candidate_marker_requires_exact_ready_generation():
    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows
            self.query = None

        def execute(self, query):
            self.query = query

        def fetchall(self):
            return self.rows

    cursor = FakeCursor([("700:2", "READY")])
    mode1_state._validate_snowflake_candidate_marker(
        cursor,
        schema="RAW_STAGE_700_2",
        expected_generation_id="700:2",
    )
    assert "TORONTO_CANDIDATE_CONTROL" in cursor.query

    with pytest.raises(RuntimeError, match="does not match its recorded run"):
        mode1_state._validate_snowflake_candidate_marker(
            FakeCursor([("699:1", "READY")]),
            schema="RAW_STAGE_700_2",
            expected_generation_id="700:2",
        )


def test_release_restore_falls_back_to_previous_valid_checkpoint(tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace

    old_bundle_name = "toronto-mode1-state-duckdb-100-1.zip"
    old_pointer_name = "toronto-mode1-state-duckdb-100-1.json"
    latest_bundle_name = "toronto-mode1-state-duckdb-200-1.zip"
    latest_pointer_name = "toronto-mode1-state-duckdb-200-1.json"
    old_bundle = b"valid-previous-bundle"
    candidate_bytes = b"restored-candidate"
    old_pointer = {
        "format_version": mode1_state.STATE_FORMAT_VERSION,
        "backend": "duckdb",
        "generation_id": "12345:1",
        "asset_name": old_bundle_name,
        "sha256": hashlib.sha256(old_bundle).hexdigest(),
    }
    latest_pointer = {
        **old_pointer,
        "asset_name": latest_bundle_name,
        "sha256": "0" * 64,
    }
    assets = [
        {"name": old_bundle_name},
        {"name": old_pointer_name},
        {"name": latest_bundle_name},
        {"name": latest_pointer_name},
    ]
    monkeypatch.setattr(mode1_state, "_release_assets", lambda _repo: assets)
    downloads = {
        old_pointer_name: json.dumps(old_pointer).encode(),
        old_bundle_name: old_bundle,
        latest_pointer_name: json.dumps(latest_pointer).encode(),
        latest_bundle_name: b"corrupt-latest-bundle",
    }
    patterns = []

    def fake_run(command, *, check=True):
        pattern = command[command.index("--pattern") + 1]
        destination = Path(command[command.index("--dir") + 1])
        destination.mkdir(parents=True, exist_ok=True)
        (destination / pattern).write_bytes(downloads[pattern])
        patterns.append(pattern)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_restore(bundle_path, restored_dir):
        assert bundle_path.name == old_bundle_name
        restored_dir.mkdir(parents=True)
        (restored_dir / "candidate.duckdb").write_bytes(candidate_bytes)
        return {"generation_id": "12345:1", "backend": "duckdb"}

    monkeypatch.setattr(mode1_state, "_run", fake_run)
    monkeypatch.setattr(mode1_state, "restore_state_bundle", fake_restore)
    restored_dir = tmp_path / "restored"
    restored = mode1_state._restore_latest_release_state(
        repository="owner/repo",
        backend="duckdb",
        working_dir=restored_dir,
    )

    assert restored["generation_id"] == "12345:1"
    assert (restored_dir / "candidate.duckdb").read_bytes() == candidate_bytes
    assert patterns == [
        latest_pointer_name,
        latest_bundle_name,
        old_pointer_name,
        old_bundle_name,
    ]


def test_release_restore_refuses_orphaned_backend_state_assets(tmp_path, monkeypatch):
    monkeypatch.setattr(
        mode1_state,
        "_release_assets",
        lambda _repo: [{"name": "toronto-mode1-state-duckdb-200-1.json"}],
    )
    with pytest.raises(RuntimeError, match="no complete pointer/bundle checkpoint"):
        mode1_state._restore_latest_release_state(
            repository="owner/repo",
            backend="duckdb",
            working_dir=tmp_path / "working",
        )
    assert not (tmp_path / "working").exists()


def test_release_restore_falls_back_when_snowflake_checkpoint_marker_is_invalid(
    tmp_path, monkeypatch
):
    import hashlib

    old_bundle = b"old-snowflake-bundle"
    latest_bundle = b"latest-snowflake-bundle"
    stems = {
        "toronto-mode1-state-snowflake-100-1": old_bundle,
        "toronto-mode1-state-snowflake-200-1": latest_bundle,
    }
    assets = [{"name": f"{stem}.zip"} for stem in stems] + [
        {"name": f"{stem}.json"} for stem in stems
    ]
    downloads = {}
    for stem, bundle_bytes in stems.items():
        downloads[f"{stem}.json"] = json.dumps(
            {
                "format_version": mode1_state.STATE_FORMAT_VERSION,
                "backend": "snowflake",
                "generation_id": "12345:1",
                "asset_name": f"{stem}.zip",
                "sha256": hashlib.sha256(bundle_bytes).hexdigest(),
            }
        ).encode()
        downloads[f"{stem}.zip"] = bundle_bytes
    monkeypatch.setattr(mode1_state, "_release_assets", lambda _repo: assets)

    def fake_run(command, **_kwargs):
        pattern = command[command.index("--pattern") + 1]
        destination = Path(command[command.index("--dir") + 1])
        destination.mkdir(parents=True, exist_ok=True)
        (destination / pattern).write_bytes(downloads[pattern])

    validated = []

    def fake_restore(bundle_path, restored_dir):
        schema = "RAW_STAGE_200_1" if "200-1" in bundle_path.name else "RAW_STAGE_100_1"
        restored_dir.mkdir(parents=True)
        return {
            "generation_id": "12345:1",
            "candidate_schema": schema,
            "last_attempt_run_id": schema.split("_")[-2],
            "last_attempt_run_attempt": schema.split("_")[-1],
        }

    def validate_schema(*, credentials, schema, expected_generation_id):
        validated.append(schema)
        if schema == "RAW_STAGE_200_1":
            raise RuntimeError("control marker mismatch")
        assert expected_generation_id == "100:1"

    monkeypatch.setattr(mode1_state, "_run", fake_run)
    monkeypatch.setattr(mode1_state, "restore_state_bundle", fake_restore)
    monkeypatch.setattr(
        mode1_state, "_validate_snowflake_checkpoint_schema", validate_schema
    )
    target = tmp_path / "snowflake-working"
    result = mode1_state._restore_latest_release_state(
        repository="owner/repo",
        backend="snowflake",
        working_dir=target,
        snowflake_credentials="test-credentials",
    )
    assert result["candidate_schema"] == "RAW_STAGE_100_1"
    assert validated == ["RAW_STAGE_200_1", "RAW_STAGE_100_1"]
    assert target.exists()


def test_mode1_workflow_guards_replay_outputs_and_dispatch_ref():
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / ".github/workflows/mode1-incremental-loader.yml"
    )
    workflow = workflow_path.read_text(encoding="utf-8")
    assert "reject_noncanonical_manual_dispatch" in workflow
    assert "github.ref != 'refs/heads/main'" in workflow
    assert workflow.count('case "$HAS_PENDING_ARCHIVES" in') == 3
    assert workflow.count("steps.replay.outputs.has_pending_archives == 'false'") == 5
    assert "persist_validated_duckdb_candidate" in workflow
    assert "persist_validated_snowflake_candidate" in workflow
    assert "CANDIDATE_DATABASE" in workflow
    assert "persist_validated_duckdb_candidate.outcome == 'success'" in workflow
    assert "persist_validated_snowflake_candidate.outcome == 'success'" in workflow
    assert '--asset-run-attempt "$((GITHUB_RUN_ATTEMPT * 2))"' in workflow
    assert '--asset-run-attempt "$((GITHUB_RUN_ATTEMPT * 2 + 1))"' in workflow
    assert "Dispatch the Snowflake serving-schema refresh" not in workflow
    assert (
        "if: always() && steps.promote_snowflake_candidate.outcome == 'success'"
        in workflow
    )
    assert (
        "if: always() && steps.promote_snowflake_candidate.outcome == 'success'"
        in workflow
    )
    assert workflow.count("steps.replay.outputs.has_pending_archives != 'true'") == 2
    assert "rebootstrap_from_mode2:" in workflow
    assert "default: false" in workflow
    assert (
        workflow.count("REBOOTSTRAP_FROM_MODE2: ${{ inputs.rebootstrap_from_mode2 }}")
        == 2
    )
    assert workflow.count("prepare_args+=(--rebootstrap-from-mode2)") == 2


@pytest.mark.parametrize("backend", ("duckdb", "snowflake"))
def test_mode2_metadata_rejects_mismatched_source_format_version(tmp_path, backend):
    ledger_path, _, _ = _mode2_fixture(tmp_path, backend=backend)

    with sqlite3.connect(ledger_path) as connection:
        connection.execute(
            """UPDATE replay_loads
               SET format_version = ?
               WHERE backend = ? AND snapshot_id = ? AND source_name = ?""",
            (1, backend, "snapshot-2", "cleared_permits"),
        )

    with pytest.raises(RuntimeError, match="duplicated or inconsistent"):
        mode1_state._mode2_generation(ledger_path, backend, "12345:1")


def _fake_snowflake_clone_session(marker_rows):
    class FakeCursor:
        def __init__(self):
            self.queries = []

        def execute(self, query, _params=None):
            self.queries.append(query)

        def fetchone(self):
            query = self.queries[-1]
            if "SELECT SCHEMA_NAME" in query:
                return None
            return ("TORONTO_INGEST_ROLE",)

        def fetchall(self):
            return marker_rows

        def close(self):
            pass

    class FakeConnection:
        def __init__(self):
            self.fake_cursor = FakeCursor()
            self.committed = False
            self.closed = False

        def cursor(self):
            return self.fake_cursor

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    class FakeTemporary:
        def cleanup(self):
            pass

    connection = FakeConnection()
    return connection, connection.fake_cursor, FakeTemporary()


def test_snowflake_promoted_raw_marker_accepts_exact_generation():
    class FakeCursor:
        def execute(self, query):
            self.query = query

        def fetchall(self):
            return [("rebuild:12345", "PROMOTED")]

    cursor = FakeCursor()
    mode1_state._validate_snowflake_promoted_raw_marker(
        cursor, expected_mode2_run_id="12345"
    )
    assert "TORONTO.RAW.TORONTO_CANDIDATE_CONTROL" in cursor.query


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [("rebuild:99999", "PROMOTED")],
        [("rebuild:12345", "READY")],
        [
            ("rebuild:12345", "PROMOTED"),
            ("rebuild:12345", "PROMOTED"),
        ],
        [
            ("rebuild:12345", "PROMOTED"),
            ("rebuild:99999", "PROMOTED"),
        ],
    ],
)
def test_snowflake_promoted_raw_marker_rejects_non_exact_state(rows):
    class FakeCursor:
        def execute(self, _query):
            pass

        def fetchall(self):
            return rows

    with pytest.raises(RuntimeError, match="expected promoted Mode 2 marker"):
        mode1_state._validate_snowflake_promoted_raw_marker(
            FakeCursor(), expected_mode2_run_id="12345"
        )


def test_mode1_snowflake_clone_uses_promoted_raw_after_marker_validation(
    monkeypatch,
):
    connection, cursor, temporary = _fake_snowflake_clone_session(
        [("rebuild:12345", "PROMOTED")]
    )
    monkeypatch.setattr(
        mode1_state,
        "_snowflake_connection",
        lambda _credentials: (connection, temporary),
    )
    from scripts import snowflake_candidate_utils

    rebase_calls = []
    monkeypatch.setattr(
        snowflake_candidate_utils,
        "rebase_pipeline_state",
        lambda _cursor, schema, source_dataset, target_dataset: (
            rebase_calls.append((schema, source_dataset, target_dataset)) or 2
        ),
    )

    schema = mode1_state._clone_snowflake_schema(
        credentials="test-credentials",
        source_schema="RAW",
        run_id="900",
        run_attempt="1",
        expected_mode2_run_id="12345",
    )

    assert schema == "RAW_STAGE_900_1"
    marker_index = next(
        i
        for i, query in enumerate(cursor.queries)
        if "FROM TORONTO.RAW.TORONTO_CANDIDATE_CONTROL" in query
    )
    clone_index = next(
        i for i, query in enumerate(cursor.queries) if "CLONE TORONTO." in query
    )
    assert marker_index < clone_index
    assert 'CLONE TORONTO."RAW"' in cursor.queries[clone_index]
    assert rebase_calls == [("RAW_STAGE_900_1", "RAW", "RAW_STAGE_900_1")]
    assert connection.committed
    assert connection.closed


def test_mode1_snowflake_clone_rejects_unpromoted_raw_before_ddl(monkeypatch):
    connection, cursor, temporary = _fake_snowflake_clone_session(
        [("rebuild:99999", "PROMOTED")]
    )
    monkeypatch.setattr(
        mode1_state,
        "_snowflake_connection",
        lambda _credentials: (connection, temporary),
    )

    with pytest.raises(RuntimeError, match="expected promoted Mode 2 marker"):
        mode1_state._clone_snowflake_schema(
            credentials="test-credentials",
            source_schema="RAW",
            run_id="901",
            run_attempt="1",
            expected_mode2_run_id="12345",
        )

    assert not any("CREATE SCHEMA" in query for query in cursor.queries)


def test_mode1_snowflake_bootstrap_uses_promoted_raw_and_preserves_ledger_candidate(
    tmp_path, monkeypatch
):
    ledger_path, _, mode2_candidate = _mode2_fixture(tmp_path, backend="snowflake")
    calls = []

    def fake_clone(**kwargs):
        calls.append(kwargs)
        return "RAW_STAGE_900_1"

    monkeypatch.setattr(mode1_state, "_clone_snowflake_schema", fake_clone)

    result = mode1_state.bootstrap_snowflake_working_state(
        mode2_ledger_path=ledger_path,
        working_dir=tmp_path / "snowflake-working",
        generation_id="12345:1",
        credentials="test-credentials",
        run_id="900",
        run_attempt="1",
    )

    assert len(calls) == 1
    assert calls[0]["source_schema"] == "RAW"
    assert calls[0]["expected_mode2_run_id"] == "12345"
    assert result["mode2_candidate"] == mode2_candidate
    assert result["mode2_promoted_source_schema"] == "TORONTO.RAW"
    assert result["candidate"] == "TORONTO.RAW_STAGE_900_1"


def test_mode1_snowflake_promoted_raw_marker_requires_exact_generation():
    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows
            self.query = ""

        def execute(self, query):
            self.query = query

        def fetchall(self):
            return self.rows

    cursor = FakeCursor([("700:2", "PROMOTED")])
    mode1_state._validate_snowflake_promoted_mode1_raw_marker(
        cursor, expected_generation_id="700:2"
    )
    assert "TORONTO.RAW.TORONTO_CANDIDATE_CONTROL" in cursor.query

    with pytest.raises(RuntimeError, match="expected Mode 1 generation"):
        mode1_state._validate_snowflake_promoted_mode1_raw_marker(
            FakeCursor([("699:1", "PROMOTED")]), expected_generation_id="700:2"
        )
    with pytest.raises(RuntimeError, match="expected promoted Mode 1 marker"):
        mode1_state._validate_snowflake_promoted_mode1_raw_marker(
            FakeCursor([("700:2", "READY")]), expected_generation_id="700:2"
        )


def test_mode1_snowflake_clone_uses_promoted_mode1_raw_after_schema_swap(monkeypatch):
    from scripts import snowflake_candidate_utils

    monkeypatch.setattr(
        snowflake_candidate_utils, "rebase_pipeline_state", lambda *args: 2
    )
    connection, cursor, temporary = _fake_snowflake_clone_session(
        [("900:2", "PROMOTED")]
    )
    monkeypatch.setattr(
        mode1_state,
        "_snowflake_connection",
        lambda _credentials: (connection, temporary),
    )

    schema = mode1_state._clone_snowflake_schema(
        credentials="test-credentials",
        source_schema="RAW",
        run_id="901",
        run_attempt="1",
        expected_promoted_generation_id="900:2",
    )

    assert schema == "RAW_STAGE_901_1"
    marker_index = next(
        index
        for index, query in enumerate(cursor.queries)
        if "FROM TORONTO.RAW.TORONTO_CANDIDATE_CONTROL" in query
    )
    clone_index = next(
        index for index, query in enumerate(cursor.queries) if "CLONE TORONTO." in query
    )
    assert marker_index < clone_index
    assert 'CLONE TORONTO."RAW"' in cursor.queries[clone_index]
    assert any("UPDATED_AT TIMESTAMP_TZ" in query for query in cursor.queries)
    assert connection.committed
    assert connection.closed


def test_validated_checkpoint_duckdb_promotion_recovery(tmp_path):
    import hashlib

    production = tmp_path / "production.duckdb"
    production.write_bytes(b"promoted-candidate")
    candidate_sha = hashlib.sha256(production.read_bytes()).hexdigest()
    manifest = {
        "production_baseline": {"backend": "duckdb", "sha256": "old"},
        "candidate_sha256": candidate_sha,
    }

    assert not mode1_state._validated_checkpoint_is_already_promoted(
        manifest=manifest,
        observed_baseline={"backend": "duckdb", "sha256": "old"},
        backend="duckdb",
        production_database=production,
    )
    assert mode1_state._validated_checkpoint_is_already_promoted(
        manifest=manifest,
        observed_baseline={"backend": "duckdb", "sha256": "new"},
        backend="duckdb",
        production_database=production,
    )

    with pytest.raises(RuntimeError, match="matches neither"):
        mode1_state._validated_checkpoint_is_already_promoted(
            manifest=manifest,
            observed_baseline={"backend": "duckdb", "sha256": "unknown"},
            backend="duckdb",
            production_database=tmp_path / "different.duckdb",
        )


def test_validated_checkpoint_snowflake_defers_to_raw_generation_marker():
    manifest = {"production_baseline": {"backend": "snowflake", "old": "signature"}}
    assert not mode1_state._validated_checkpoint_is_already_promoted(
        manifest=manifest,
        observed_baseline={"backend": "snowflake", "new": "signature"},
        backend="snowflake",
        production_database=None,
    )


def test_mode1_snowflake_generation_prefers_candidate_identity():
    manifest = {
        "candidate_generation_id": "900:2",
        "last_attempt_run_id": "901",
        "last_attempt_run_attempt": "1",
    }
    assert mode1_state._manifest_snowflake_generation(manifest) == "900:2"


def test_snowflake_raw_mode1_marker_state_accepts_ready_and_promoted():
    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, _query):
            pass

        def fetchall(self):
            return self.rows

    assert (
        mode1_state._snowflake_raw_mode1_marker_state(
            FakeCursor([("900:2", "READY")]), expected_generation_id="900:2"
        )
        == "READY"
    )
    assert (
        mode1_state._snowflake_raw_mode1_marker_state(
            FakeCursor([("900:2", "PROMOTED")]), expected_generation_id="900:2"
        )
        == "PROMOTED"
    )
    with pytest.raises(RuntimeError):
        mode1_state._snowflake_raw_mode1_marker_state(
            FakeCursor([("899:1", "READY")]), expected_generation_id="900:2"
        )


def test_mode1_state_asset_attempt_separates_checkpoints():
    assert mode1_state._state_asset_attempt("1") == "1"
    assert mode1_state._state_asset_attempt("1", "2") == "2"
    assert mode1_state._state_asset_attempt("1", "3") == "3"
    with pytest.raises(ValueError, match="digits only"):
        mode1_state._state_asset_attempt("1", "validated")
