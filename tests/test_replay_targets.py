from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import replay_targets, replay_warehouse_ledger
from scripts.replay_ledger import ReplayLedger


@pytest.fixture(autouse=True)
def _enable_snowflake_operations(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "true")


def _install_fake_scd2_analysis(monkeypatch, module, *, analysis=None):
    from types import SimpleNamespace

    module._resolve_scd2_key_columns = lambda _path, source: tuple(source.primary_key)
    module._analyze_scd2_snapshot = lambda *args, **kwargs: (
        analysis
        or SimpleNamespace(
            invalid_key_row_count=0,
            business_key_count=0,
            duplicate_row_count=0,
            bad_row_count=0,
            quarantine_rows=(),
        )
    )
    monkeypatch.setitem(
        __import__("sys").modules, "toronto_housing_ingestion.dlt_loader", module
    )


def _config():
    return SimpleNamespace(
        required_columns=("permit_num",),
        primary_key=("permit_num", "revision_num", "permit_type"),
        min_row_count=1000,
    )


def _archive(snapshot_id: str, minute: int):
    extracted = datetime(2026, 11, 1, 12, minute, tzinfo=UTC)
    source = SimpleNamespace(
        extracted_at=extracted,
        manifest={},
        parquet_sha256=f"{minute:064x}",
        row_count=3000,
        columns=("permit_num", "revision_num", "permit_type"),
        error=None,
    )
    return SimpleNamespace(
        path=Path(f"{snapshot_id}.zip"),
        format_version=1,
        snapshot_id=snapshot_id,
        extracted_at=extracted,
        source_commit="a" * 40,
        run_id=str(100 + minute),
        run_attempt=1,
        release_tag="toronto-raw-snapshot-archive-2026-11",
        asset_name=f"toronto-raw-snapshot-bundle-{100 + minute}-1.zip",
        archive_sha256="b" * 64,
        sources={"active_permits": source, "cleared_permits": source},
        order=(extracted, 100 + minute, 1, snapshot_id),
    )


class _Target:
    backend = "duckdb"

    def __init__(self, fail_source: str | None = None):
        self.fail_source = fail_source
        self.loaded: list[tuple[str, str]] = []
        self.validated = False
        self.finalized = False

    def load(self, archive, source_name, parquet_path, source_config):
        self.loaded.append((source_name, archive.snapshot_id))
        if source_name == self.fail_source and archive.snapshot_id == "s1":
            raise RuntimeError("injected source failure")
        return {"rows": 3}

    def validate_and_export(self, source_configs, output_dir):
        self.validated = True
        return {
            "status": "passed",
            "sources": {
                "active_permits": {"status": "passed"},
                "cleared_permits": {"status": "passed"},
            },
        }

    def finalize(self):
        self.finalized = True


class _PersistentTarget(_Target):
    def __init__(self, fail_source: str | None = None):
        super().__init__(fail_source)
        self.warehouse_status: dict[tuple[str, str], str] = {}

    def register_archive(self, archive):
        for source_name in ("active_permits", "cleared_permits"):
            self.warehouse_status.setdefault(
                (archive.snapshot_id, source_name), "pending"
            )

    def snapshot_status(self, snapshot_id, source_name):
        return self.warehouse_status.get((snapshot_id, source_name))

    def set_snapshot_status(self, archive, source_name, status, **kwargs):
        self.warehouse_status[(archive.snapshot_id, source_name)] = status


def test_coordinator_persists_each_source_status_to_candidate_ledger(
    tmp_path, monkeypatch
):
    archive = _archive("s1", 1)
    target = _PersistentTarget(fail_source="active_permits")
    monkeypatch.setattr(
        replay_targets,
        "extract_source",
        lambda archive, source, destination: Path(destination) / f"{source}.parquet",
    )

    report = replay_targets.replay_backend(
        [archive],
        source_configs={
            "active_permits": _config(),
            "cleared_permits": _config(),
        },
        target=target,
        ledger_path=tmp_path / "ledger.sqlite",
        staging_root=tmp_path / "staging",
    )

    assert report["status"] == "failed"
    assert target.snapshot_status("s1", "active_permits") == "failed"
    assert target.snapshot_status("s1", "cleared_permits") == "succeeded"


def test_duckdb_candidate_must_be_inside_recovery_root(tmp_path):
    root = tmp_path / "recovery"
    assert (
        replay_targets.candidate_database_path(
            root / "candidate.duckdb", candidate_root=root
        )
        == (root / "candidate.duckdb").resolve()
    )
    with pytest.raises(ValueError, match="inside"):
        replay_targets.candidate_database_path(
            tmp_path / "production.duckdb", candidate_root=root
        )
    with pytest.raises(ValueError, match="production"):
        replay_targets.candidate_database_path(
            root / "production.duckdb",
            candidate_root=root,
            production_database=root / "production.duckdb",
        )


def test_snowflake_candidate_schema_is_mode_specific():
    assert replay_targets.REBUILD_SNOWFLAKE_SCHEMA_RE.fullmatch(
        "TORONTO_SNAPSHOT_REPLAY_123456"
    )
    assert replay_targets.INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch(
        "RAW_STAGE_123456_2"
    )
    assert not replay_targets.INCREMENTAL_SNOWFLAKE_SCHEMA_RE.fullmatch("RAW")
    assert not replay_targets.REBUILD_SNOWFLAKE_SCHEMA_RE.fullmatch(
        "RAW_STAGE_123456_2"
    )


def test_duckdb_destination_root_is_mode_specific(tmp_path):
    rebuild_root = tmp_path / "rebuild"
    incremental_root = tmp_path / "incremental"
    rebuild = replay_targets.candidate_database_path(
        rebuild_root / "candidate.duckdb", mode="rebuild", candidate_root=rebuild_root
    )
    incremental = replay_targets.candidate_database_path(
        incremental_root / "candidate.duckdb",
        mode="incremental",
        candidate_root=incremental_root,
    )
    assert rebuild == (rebuild_root / "candidate.duckdb").resolve()
    assert incremental == (incremental_root / "candidate.duckdb").resolve()
    with pytest.raises(ValueError, match="incremental candidate"):
        replay_targets.candidate_database_path(
            rebuild_root / "candidate.duckdb",
            mode="incremental",
            candidate_root=incremental_root,
        )


def test_source_boundary_uses_v2_source_extraction_start():
    started = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
    archive = _archive("s2", 2)
    archive.format_version = 2
    archive.sources["active_permits"].extracted_at = started
    archive.sources["active_permits"].manifest = {
        "extraction_started_at_utc": started.isoformat(),
        "extraction_completed_at_utc": (started + timedelta(seconds=10)).isoformat(),
    }
    assert replay_targets.source_boundary(archive, "active_permits") == started


def test_failed_active_stream_does_not_stop_cleared_stream(tmp_path, monkeypatch):
    archives = [_archive("s1", 1), _archive("s2", 2)]
    target = _Target(fail_source="active_permits")
    monkeypatch.setattr(
        replay_targets,
        "extract_source",
        lambda archive, source, destination: Path(destination) / f"{source}.parquet",
    )
    configs = {
        "active_permits": _config(),
        "cleared_permits": _config(),
    }

    report = replay_targets.replay_backend(
        archives,
        source_configs=configs,
        target=target,
        ledger_path=tmp_path / "ledger.sqlite",
        staging_root=tmp_path / "staging",
    )

    assert report["status"] == "failed"
    assert report["sources"]["active_permits"]["failed"]
    assert report["sources"]["cleared_permits"]["status"] == "passed"
    assert target.validated is True
    assert target.finalized is True
    assert [item for item in target.loaded if item[0] == "cleared_permits"] == [
        ("cleared_permits", "s1"),
        ("cleared_permits", "s2"),
    ]


def test_replay_retry_skips_success_and_retries_only_failed_stream(
    tmp_path, monkeypatch
):
    archives = [_archive("s1", 1), _archive("s2", 2)]
    ledger_path = tmp_path / "ledger.sqlite"
    ledger = ReplayLedger(ledger_path)
    ledger.register_archives("duckdb", archives)
    ledger.mark("duckdb", "active_permits", "s1", "loading")
    ledger.mark("duckdb", "active_permits", "s1", "failed", error="test")
    for archive in archives:
        ledger.mark("duckdb", "cleared_permits", archive.snapshot_id, "loading")
        ledger.mark("duckdb", "cleared_permits", archive.snapshot_id, "succeeded")
    monkeypatch.setattr(
        replay_targets,
        "extract_source",
        lambda archive, source, destination: Path(destination) / f"{source}.parquet",
    )
    target = _Target()
    report = replay_targets.replay_backend(
        archives,
        source_configs={"active_permits": _config(), "cleared_permits": _config()},
        target=target,
        ledger_path=ledger_path,
        staging_root=tmp_path / "staging",
    )

    assert report["status"] == "passed"
    assert target.loaded == [
        ("active_permits", "s1"),
        ("active_permits", "s2"),
    ]
    assert report["sources"]["cleared_permits"]["skipped"] == 2


def test_empty_backlog_still_validates_existing_candidate(tmp_path):
    target = _Target()
    report = replay_targets.replay_backend(
        [],
        source_configs={"active_permits": _config(), "cleared_permits": _config()},
        target=target,
        ledger_path=tmp_path / "ledger.sqlite",
        staging_root=tmp_path / "staging",
    )
    assert report["status"] == "passed"
    assert target.validated and target.finalized


def test_source_contract_rejects_manifest_key_drift():
    archive = _archive("s3", 3)
    archive.sources["active_permits"].manifest = {
        "business_key": ["permit_num", "wrong_key"]
    }
    with pytest.raises(ValueError, match="business key disagrees"):
        replay_targets.validate_source_contract(archive, "active_permits", _config())


def test_source_contract_rejects_missing_configured_column():
    archive = _archive("s4", 4)
    archive.sources["active_permits"].columns = ("permit_num",)
    with pytest.raises(ValueError, match="missing configured columns"):
        replay_targets.validate_source_contract(archive, "active_permits", _config())


def test_snowflake_credentials_require_expected_role_and_warehouse():
    credentials = (
        "snowflake://user@bihaste-tu44125/TORONTO"
        "?role=TORONTO_INGEST_ROLE&warehouse=TORONTO_XS_WH&private_key=YWJj"
    )
    values, key_bytes, passphrase = (
        replay_targets.SnowflakeReplayTarget._parse_credentials(credentials)
    )
    assert values["database"] == "TORONTO"
    assert values["role"] == "TORONTO_INGEST_ROLE"
    assert values["warehouse"] == "TORONTO_XS_WH"
    assert key_bytes == b"abc"
    assert passphrase is None

    with pytest.raises(ValueError, match="requires TORONTO_INGEST_ROLE"):
        replay_targets.SnowflakeReplayTarget._parse_credentials(
            credentials.replace("TORONTO_INGEST_ROLE", "PUBLIC")
        )


def test_validation_query_contains_interval_and_duplicate_checks():
    queries = replay_targets.validation_queries(
        "raw.raw_active_permits",
        ("permit_num", "revision_num", "permit_type"),
        snowflake=False,
    )
    assert "previous_max_to > _dlt_valid_from" in queries["overlap"]
    assert "GROUP BY" in queries["duplicate"]
    assert "_dlt_valid_to IS NULL" in queries["duplicate"]
    assert "null_business" not in queries["null_keys"]


def test_snowflake_credentials_reject_unexpected_account():
    credentials = (
        "snowflake://user@other-account/TORONTO"
        "?role=TORONTO_INGEST_ROLE&warehouse=TORONTO_XS_WH&private_key=YWJj"
    )
    with pytest.raises(ValueError, match="unexpected account"):
        replay_targets.SnowflakeReplayTarget._parse_credentials(credentials)


def test_snowflake_session_context_rejected_before_candidate_ddl():
    with pytest.raises(ValueError, match="Unexpected Snowflake replay session"):
        replay_targets._validate_snowflake_account_context(
            "PUBLIC",
            "TORONTO",
            "TORONTO_XS_WH",
            "BIHASTE",
            "TU44125",
        )


def test_snowflake_session_context_accepts_expected_project_session():
    replay_targets._validate_snowflake_account_context(
        "TORONTO_INGEST_ROLE",
        "TORONTO",
        "TORONTO_XS_WH",
        "BIHASTE",
        "TU44125",
    )


def test_persisted_warehouse_ledger_blocks_older_snapshot_after_newer_write():
    older = _archive("older", 1)
    newer = _archive("newer", 2)
    rows = [
        (
            newer.snapshot_id,
            newer.run_id,
            newer.run_attempt,
            newer.extracted_at,
            "failed",
        )
    ]

    with pytest.raises(RuntimeError, match="Out-of-order"):
        replay_warehouse_ledger._assert_replay_order_rows(
            rows, older, "duckdb", "active_permits"
        )


def test_persisted_warehouse_ledger_allows_next_snapshot_after_prior_success():
    older = _archive("older", 1)
    newer = _archive("newer", 2)
    rows = [
        (
            older.snapshot_id,
            older.run_id,
            older.run_attempt,
            older.extracted_at,
            "succeeded",
        )
    ]

    replay_warehouse_ledger._assert_replay_order_rows(
        rows, newer, "duckdb", "cleared_permits"
    )


def test_persisted_warehouse_ledger_blocks_gap_in_earlier_history():
    older = _archive("older", 1)
    newer = _archive("newer", 2)
    rows = [
        (
            older.snapshot_id,
            older.run_id,
            older.run_attempt,
            older.extracted_at,
            "pending",
        )
    ]

    with pytest.raises(RuntimeError, match="earlier incomplete"):
        replay_warehouse_ledger._assert_replay_order_rows(
            rows, newer, "snowflake", "active_permits"
        )


def test_input_stage_config_loads_full_source_without_warehouse_date_filter():
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Config:
        warehouse_strategy: str = "scd2"
        warehouse_since: str | None = "2020-01-01"
        warehouse_date_column: str | None = "APPLICATION_DATE"

    staged = replay_targets._input_stage_config(Config())

    assert staged.warehouse_strategy == "replace"
    assert staged.warehouse_since is None
    assert staged.warehouse_date_column is None


def test_input_stage_validation_rejects_row_count_or_schema_mismatch():
    replay_targets._validate_staged_snapshot(
        source_name="active_permits",
        expected_rows=2,
        expected_columns=("permit_num", "revision_num"),
        actual_rows=2,
        actual_columns=("PERMIT_NUM", "REVISION_NUM", "_dlt_id"),
    )
    with pytest.raises(ValueError, match="row count"):
        replay_targets._validate_staged_snapshot(
            source_name="active_permits",
            expected_rows=2,
            expected_columns=("permit_num",),
            actual_rows=1,
            actual_columns=("permit_num",),
        )


def test_mode1_preflight_uses_canonical_key_analysis_and_reports_quarantine(
    monkeypatch,
):
    from types import ModuleType, SimpleNamespace

    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    analysis = SimpleNamespace(
        invalid_key_row_count=1,
        business_key_count=1,
        duplicate_row_count=2,
        bad_row_count=3,
        quarantine_rows=(
            {"reason": "invalid_business_key"},
            {"reason": "conflicting_duplicate_business_key"},
            {"reason": "conflicting_duplicate_business_key"},
            {"reason": "identical_duplicate_record"},
            {"reason": "identical_duplicate_record"},
        ),
    )
    _install_fake_scd2_analysis(monkeypatch, module, analysis=analysis)
    config = SimpleNamespace(
        primary_key=("permit_num",),
        warehouse_date_column="APPLICATION_DATE",
        warehouse_since="2020-01-01",
        max_row_change_percent=50,
    )

    result = replay_targets._analyse_mode1_snapshot(
        Path("snapshot.parquet"),
        "active_permits",
        config,
        warehouse_rows=150,
        baseline_current_rows=100,
    )

    assert result == {
        "status": "passed",
        "warehouse_rows": 150,
        "baseline_current_rows": 100,
        "row_change_percent": 50.0,
        "max_row_change_percent": 50.0,
        "invalid_business_key_rows": 1,
        "conflicting_duplicate_key_groups": 1,
        "conflicting_duplicate_rows": 2,
        "identical_duplicate_rows": 2,
        "quarantined_bad_rows": 3,
        "quarantined_total_rows": 5,
        "key_handling": "quarantine_before_scd2",
    }


def test_mode1_row_change_gate_rejects_spike_before_scd2(monkeypatch):
    from types import ModuleType, SimpleNamespace

    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    _install_fake_scd2_analysis(monkeypatch, module)
    config = SimpleNamespace(
        primary_key=("permit_num",),
        warehouse_date_column=None,
        warehouse_since=None,
        max_row_change_percent=50,
    )

    with pytest.raises(ValueError, match="exceeding the configured 50.00% limit"):
        replay_targets._analyse_mode1_snapshot(
            Path("snapshot.parquet"),
            "active_permits",
            config,
            warehouse_rows=151,
            baseline_current_rows=100,
        )


def test_mode1_row_change_gate_fails_closed_without_a_baseline(monkeypatch):
    from types import ModuleType, SimpleNamespace

    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    _install_fake_scd2_analysis(monkeypatch, module)
    config = SimpleNamespace(
        primary_key=("permit_num",),
        warehouse_date_column=None,
        warehouse_since=None,
        max_row_change_percent=50,
    )

    with pytest.raises(ValueError, match="empty production-derived candidate"):
        replay_targets._analyse_mode1_snapshot(
            Path("snapshot.parquet"),
            "active_permits",
            config,
            warehouse_rows=1,
            baseline_current_rows=0,
        )


def test_mode1_preflight_must_match_the_scd2_quarantine_result():
    from types import SimpleNamespace

    preflight = {
        "quarantined_total_rows": 4,
        "quarantined_bad_rows": 3,
        "conflicting_duplicate_key_groups": 1,
    }
    result = SimpleNamespace(
        quarantined_row_count=4,
        quarantined_bad_row_count=3,
        quarantined_business_key_count=1,
    )
    replay_targets._verify_scd2_quarantine_matches_preflight(
        "active_permits", preflight, result
    )
    result.quarantined_bad_row_count = 2
    with pytest.raises(ValueError, match="differ from the staged-source preflight"):
        replay_targets._verify_scd2_quarantine_matches_preflight(
            "active_permits", preflight, result
        )


def test_warehouse_window_filter_matches_source_date_cutoff_for_both_backends():
    from types import SimpleNamespace

    config = SimpleNamespace(
        warehouse_date_column="APPLICATION_DATE", warehouse_since="2020-01-01"
    )
    duckdb_where, duckdb_value = replay_targets._warehouse_window_filter(
        config, snowflake=False
    )
    snowflake_where, snowflake_value = replay_targets._warehouse_window_filter(
        config, snowflake=True
    )
    assert 'SUBSTR(CAST("APPLICATION_DATE" AS VARCHAR), 1, 10) >= ?' in duckdb_where
    assert 'SUBSTR(TO_VARCHAR("APPLICATION_DATE"), 1, 10) >= %s' in snowflake_where
    assert duckdb_value == snowflake_value == "2020-01-01"
    with pytest.raises(ValueError, match="missing archived columns"):
        replay_targets._validate_staged_snapshot(
            source_name="active_permits",
            expected_rows=2,
            expected_columns=("permit_num", "revision_num"),
            actual_rows=2,
            actual_columns=("permit_num",),
        )


def test_snowflake_input_stage_schema_is_derived_only_from_incremental_candidate():
    assert replay_targets.snowflake_input_stage_schema("RAW_STAGE_123456_2") == (
        "RAW_INPUT_STAGE_123456_2"
    )
    assert (
        replay_targets.snowflake_input_stage_schema("TORONTO_SNAPSHOT_REPLAY_123456")
        is None
    )
    assert replay_targets.snowflake_input_stage_schema("RAW") is None


def test_incremental_duckdb_load_stages_before_candidate_scd2(tmp_path, monkeypatch):
    import sys
    from types import ModuleType

    order = []
    result = SimpleNamespace(
        table_name="raw_active_permits",
        total_rows=4,
        current_rows=4,
        quarantined_row_count=0,
        quarantined_bad_row_count=0,
        quarantined_business_key_count=0,
        quarantine_sha256=None,
    )

    def fake_load_scd2(**kwargs):
        order.append(
            ("candidate_scd2", kwargs["dataset_name"], kwargs["pipeline_name"])
        )
        return result

    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    module.load_scd2 = fake_load_scd2
    monkeypatch.setitem(sys.modules, "toronto_housing_ingestion.dlt_loader", module)

    target = object.__new__(replay_targets.DuckDBReplayTarget)
    target.mode = "incremental"
    target.database = tmp_path / "candidate.duckdb"
    target.dataset = "raw"
    target._stage_input = lambda *args: (
        order.append(("input_stage",)) or {"schema": "snapshot_input_stage", "rows": 4}
    )

    output = target.load(
        _archive("s1", 1),
        "active_permits",
        tmp_path / "active.parquet",
        SimpleNamespace(
            warehouse_table="raw_active_permits", primary_key=("permit_num",)
        ),
    )

    assert [item[0] for item in order] == ["input_stage", "candidate_scd2"]
    assert output["input_stage"] == {"schema": "snapshot_input_stage", "rows": 4}
    assert order[1][1] == "raw"
    assert order[1][2] == replay_targets.PRODUCTION_PIPELINES["active_permits"]


def test_rebuild_duckdb_load_does_not_create_an_input_stage(tmp_path, monkeypatch):
    import sys
    from types import ModuleType

    calls = []
    result = SimpleNamespace(
        table_name="raw_active_permits",
        total_rows=4,
        current_rows=4,
        quarantined_row_count=0,
        quarantined_bad_row_count=0,
        quarantined_business_key_count=0,
        quarantine_sha256=None,
    )
    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    module.load_scd2 = lambda **kwargs: calls.append(kwargs) or result
    monkeypatch.setitem(sys.modules, "toronto_housing_ingestion.dlt_loader", module)

    target = object.__new__(replay_targets.DuckDBReplayTarget)
    target.mode = "rebuild"
    target.database = tmp_path / "rebuild.duckdb"
    target.dataset = "raw"

    output = target.load(
        _archive("s1", 1),
        "active_permits",
        tmp_path / "active.parquet",
        SimpleNamespace(
            warehouse_table="raw_active_permits", primary_key=("permit_num",)
        ),
    )

    assert len(calls) == 1
    assert calls[0]["credentials"] == str(target.database)
    assert (
        calls[0]["pipeline_name"]
        == replay_targets.PRODUCTION_PIPELINES["active_permits"]
    )
    assert output["input_stage"] is None


def test_incremental_snowflake_load_stages_before_candidate_scd2(tmp_path, monkeypatch):
    import sys
    from types import ModuleType

    order = []
    result = SimpleNamespace(
        table_name="raw_active_permits",
        quarantined_row_count=0,
        quarantined_bad_row_count=0,
        quarantined_business_key_count=0,
        quarantine_sha256=None,
    )

    def fake_load_scd2(**kwargs):
        order.append(
            ("candidate_scd2", kwargs["dataset_name"], kwargs["pipeline_name"])
        )
        return result

    module = ModuleType("toronto_housing_ingestion.dlt_loader")
    module.load_scd2 = fake_load_scd2
    monkeypatch.setitem(sys.modules, "toronto_housing_ingestion.dlt_loader", module)

    target = object.__new__(replay_targets.SnowflakeReplayTarget)
    target.mode = "incremental"
    target.credentials = "snowflake://test"
    target.schema = "RAW_STAGE_123456_2"
    target.input_stage_schema = "RAW_INPUT_STAGE_123456_2"
    target._stage_input = lambda *args: (
        order.append(("input_stage",))
        or {"schema": "RAW_INPUT_STAGE_123456_2", "rows": 4}
    )

    output = target.load(
        _archive("s1", 1),
        "active_permits",
        tmp_path / "active.parquet",
        SimpleNamespace(
            warehouse_table="raw_active_permits", primary_key=("permit_num",)
        ),
    )

    assert [item[0] for item in order] == ["input_stage", "candidate_scd2"]
    assert order[1][1] == "RAW_STAGE_123456_2"
    assert order[1][2] == replay_targets.PRODUCTION_PIPELINES["active_permits"]
    assert output["input_stage"] == {"schema": "RAW_INPUT_STAGE_123456_2", "rows": 4}


def test_duckdb_incremental_materializes_and_validates_full_input_stage(
    tmp_path, monkeypatch
):
    import sys
    from dataclasses import dataclass
    from types import ModuleType

    calls = []

    @dataclass(frozen=True)
    class Config:
        warehouse_table: str = "raw_active_permits"
        warehouse_strategy: str = "scd2"
        warehouse_since: str | None = "2020-01-01"
        warehouse_date_column: str | None = "APPLICATION_DATE"
        max_row_change_percent: float | None = None
        required_columns: tuple[str, ...] = ("permit_num",)
        primary_key: tuple[str, ...] = ("permit_num",)
        type_hints: dict = None
        warehouse_load: bool = True

    class FakeCursor:
        description = [("PERMIT_NUM",), ("REVISION_NUM",), ("PERMIT_TYPE",)]

        def fetchone(self):
            return (3000,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query, params=None):
            calls.append(("query", query, params))
            return FakeCursor()

    duckdb = ModuleType("duckdb")
    duckdb.connect = lambda *args, **kwargs: FakeConnection()
    monkeypatch.setitem(sys.modules, "duckdb", duckdb)

    def fake_load_scd2(**kwargs):
        calls.append(("stage_load", kwargs))

    loader = ModuleType("toronto_housing_ingestion.dlt_loader")
    loader.load_scd2 = fake_load_scd2
    _install_fake_scd2_analysis(monkeypatch, loader)

    target = object.__new__(replay_targets.DuckDBReplayTarget)
    target.mode = "incremental"
    target.input_stage_database = tmp_path / "incoming.input-stage.duckdb"
    target._current_scd2_rows = lambda _source: 3000
    archive = _archive("s1", 1)
    result = target._stage_input(
        archive,
        "active_permits",
        tmp_path / "active.parquet",
        Config(type_hints={}),
    )

    stage_call = next(value[1] for value in calls if value[0] == "stage_load")
    assert stage_call["dataset_name"] == "snapshot_input_stage"
    assert stage_call["destination_kind"] == "duckdb"
    assert stage_call["source"].warehouse_strategy == "replace"
    assert stage_call["source"].warehouse_since is None
    assert stage_call["source"].warehouse_date_column is None
    assert result["schema"] == "snapshot_input_stage"
    assert result["table"] == "raw_active_permits"
    assert result["rows"] == 3000
    assert result["quality"]["status"] == "passed"
    assert result["quality"]["warehouse_rows"] == 3000
    assert result["quality"]["baseline_current_rows"] == 3000


def test_snowflake_incremental_materializes_owned_input_stage_before_scd2(
    tmp_path, monkeypatch
):
    from contextlib import contextmanager
    from dataclasses import dataclass
    from types import ModuleType

    calls = []

    @dataclass(frozen=True)
    class Config:
        warehouse_table: str = "raw_active_permits"
        warehouse_strategy: str = "scd2"
        warehouse_since: str | None = "2020-01-01"
        warehouse_date_column: str | None = "APPLICATION_DATE"
        max_row_change_percent: float | None = None
        required_columns: tuple[str, ...] = ("permit_num",)
        primary_key: tuple[str, ...] = ("permit_num",)
        type_hints: dict = None
        warehouse_load: bool = True

    class FakeCursor:
        description = [("PERMIT_NUM",), ("REVISION_NUM",), ("PERMIT_TYPE",)]

        def __init__(self):
            self.last_query = ""
            self.owner_queries = 0

        def execute(self, query, params=None):
            self.last_query = query
            calls.append(("sql", query, params))
            return self

        def fetchone(self):
            if "CURRENT_ROLE()" in self.last_query:
                return (
                    "TORONTO_INGEST_ROLE",
                    "TORONTO",
                    "TORONTO_XS_WH",
                    "bihaste",
                    "tu44125",
                )
            if "SCHEMA_OWNER" in self.last_query:
                self.owner_queries += 1
                return None if self.owner_queries == 1 else ("TORONTO_INGEST_ROLE",)
            if "COUNT(*)" in self.last_query:
                return (3000,)
            return None

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append(("commit",))

    @contextmanager
    def fake_session():
        yield FakeConnection()

    def fake_load_scd2(**kwargs):
        calls.append(("stage_load", kwargs))

    loader = ModuleType("toronto_housing_ingestion.dlt_loader")
    loader.load_scd2 = fake_load_scd2
    _install_fake_scd2_analysis(monkeypatch, loader)

    target = object.__new__(replay_targets.SnowflakeReplayTarget)
    target.mode = "incremental"
    target.credentials = "snowflake://test"
    target.input_stage_schema = "RAW_INPUT_STAGE_123456_2"
    target._current_scd2_rows = lambda _source: 3000
    target._session = fake_session

    result = target._stage_input(
        _archive("s1", 1),
        "active_permits",
        tmp_path / "active.parquet",
        Config(type_hints={}),
    )

    statements = [entry[1] for entry in calls if entry[0] == "sql"]
    assert any(
        'CREATE SCHEMA TORONTO."RAW_INPUT_STAGE_123456_2"' in q for q in statements
    )
    stage_call = next(entry[1] for entry in calls if entry[0] == "stage_load")
    assert stage_call["dataset_name"] == "RAW_INPUT_STAGE_123456_2"
    assert stage_call["source"].warehouse_strategy == "replace"
    assert stage_call["source"].warehouse_since is None
    assert result["schema"] == "RAW_INPUT_STAGE_123456_2"
    assert result["table"] == "RAW_ACTIVE_PERMITS"
    assert result["rows"] == 3000
    assert result["quality"]["status"] == "passed"
    assert result["quality"]["warehouse_rows"] == 3000
    assert result["quality"]["baseline_current_rows"] == 3000


def test_snowflake_replay_schema_must_be_owned_by_ingest_role():
    replay_targets._validate_snowflake_schema_owner(
        "TORONTO_SNAPSHOT_REPLAY_123456", ("TORONTO_INGEST_ROLE",)
    )

    with pytest.raises(ValueError, match="must be owned by TORONTO_INGEST_ROLE"):
        replay_targets._validate_snowflake_schema_owner(
            "TORONTO_SNAPSHOT_REPLAY_123456", ("PUBLIC",)
        )

    with pytest.raises(ValueError, match="must be owned by TORONTO_INGEST_ROLE"):
        replay_targets._validate_snowflake_schema_owner(
            "TORONTO_SNAPSHOT_REPLAY_123456", None
        )


def test_snowflake_replay_target_rejects_disabled_gate(monkeypatch):
    for value in (None, "false", "TRUE"):
        if value is None:
            monkeypatch.delenv("SNOWFLAKE_OPERATIONS_ENABLED", raising=False)
        else:
            monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", value)

        with pytest.raises(PermissionError, match="SNOWFLAKE_OPERATIONS_ENABLED"):
            replay_targets.SnowflakeReplayTarget(
                "unused",
                "TORONTO_SNAPSHOT_REPLAY_123",
            )


def test_mode2_backend_selection_and_snowflake_gate_are_independent():
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/snapshot-replay.yml").read_text(
        encoding="utf-8"
    )

    duckdb_job = workflow.split("  duckdb:", 1)[1].split("  snowflake:", 1)[0]
    snowflake_job = workflow.split("  snowflake:", 1)[1].split("  compare:", 1)[0]
    compare_job = workflow.split("  compare:", 1)[1].split("  compare-existing:", 1)[0]

    assert "SNOWFLAKE_OPERATIONS_ENABLED" not in duckdb_job
    assert "vars.SNOWFLAKE_OPERATIONS_ENABLED == 'true'" in snowflake_job
    assert "vars.SNOWFLAKE_OPERATIONS_ENABLED == 'true'" in compare_job
    assert "name: Load DuckDB (rebuild)" in duckdb_job
    assert "name: Load Snowflake (rebuild)" in snowflake_job
    assert "--mode rebuild" in duckdb_job
    assert "--mode rebuild" in snowflake_job
    assert "inputs.mode" not in workflow
    assert "deploy-dbt-docs" not in workflow
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "snapshot-replay.yml"
    ).read_text(encoding="utf-8")

    duckdb_job = workflow.split("  duckdb:", 1)[1].split("  snowflake:", 1)[0]
    snowflake_job = workflow.split("  snowflake:", 1)[1].split("  compare:", 1)[0]
    compare_job = workflow.split("  compare:", 1)[1].split("  deploy-dbt-docs:", 1)[0]

    assert "SNOWFLAKE_OPERATIONS_ENABLED" not in duckdb_job
    assert "vars.SNOWFLAKE_OPERATIONS_ENABLED == 'true'" in snowflake_job
    assert "vars.SNOWFLAKE_OPERATIONS_ENABLED == 'true'" in compare_job
    assert "needs.duckdb.result == 'success'" in compare_job
