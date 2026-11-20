from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.validate_snapshot_replay import (
    validate_backend_report,
    validate_reports,
)


def _report(backend: str, *, checksum: str = "b" * 64) -> dict:
    records = []
    snapshots = (("snapshot-a", 10, "1001"), ("snapshot-b", 20, "1002"))
    for snapshot_id, minute, run_id in snapshots:
        for source_name in ("active_permits", "cleared_permits"):
            records.append(
                {
                    "source_name": source_name,
                    "snapshot_id": snapshot_id,
                    "format_version": 2,
                    "order_timestamp": 1793534400.0 + minute * 60,
                    "run_id": run_id,
                    "run_attempt": 1,
                    "source_commit": "a" * 40,
                    "archive_sha256": checksum,
                    "release_tag": "toronto-raw-snapshot-v2-archive-2026-11",
                    "asset_name": f"toronto-raw-snapshot-v2-bundle-{snapshot_id}.zip",
                    "status": "succeeded",
                    "warehouse_status": "succeeded",
                    "quarantined_row_count": 0,
                    "quarantined_bad_row_count": 0,
                    "quarantined_business_key_count": 0,
                    "quarantine_sha256": None,
                }
            )
    source_metrics = {
        "status": "passed",
        "total": 10,
        "current": 8,
        "expired": 2,
        "invalid_intervals": 0,
        "overlapping_intervals": 0,
        "duplicate_current_business_keys": 0,
        "null_business_keys": 0,
        "export": f"{backend}/exports/permit_history.csv.gz",
        "export_rows": 10,
    }
    return {
        "backend": backend,
        "status": "passed",
        "mode": "rebuild",
        "history_scope": "full",
        "full_history": True,
        "history_scope_basis": "versioned_inventory_verified_v1_v2_release_archives",
        "explicit_promotion_required": True,
        "warehouse_input_staging": "not_used",
        "snapshot_count_discovered": 2,
        "snapshot_count_selected": 2,
        "snapshot_count_pending_before_load": 2,
        "selection": {
            "from_date": None,
            "to_date": None,
            "limit": None,
            "limit_semantics": "most_recent_n_then_replay_chronologically",
            "ordering": "extracted_at_utc_ascending",
            "limit_applied_before_retry_skips": True,
        },
        "candidate": "candidate",
        "sources": {
            name: {"status": "passed", "failed": [], "blocked": []}
            for name in ("active_permits", "cleared_permits")
        },
        "history_validation": {
            "status": "passed",
            "sources": {
                name: deepcopy(source_metrics)
                for name in ("active_permits", "cleared_permits")
            },
        },
        "ledger_entries": records,
    }


def test_one_backend_can_be_validated_without_a_second_backend():
    result = validate_backend_report(_report("duckdb"), "duckdb")

    assert result["mode"] == "rebuild"
    assert result["history_scope"] == "full"
    assert result["full_history"] is True

    with pytest.raises(ValueError, match="expected backend"):
        validate_backend_report(_report("snowflake"), "duckdb")


def test_identical_backend_reports_pass_with_complete_ledger_and_history():
    result = validate_reports(_report("duckdb"), _report("snowflake"))

    assert result["status"] == "passed"
    assert result["snapshot_count"] == 2
    assert result["source_ledger_entries"] == 4
    assert result["archive_provenance"] == "passed"
    assert result["backend_snapshot_coverage"] == "identical"
    assert result["quarantine_parity"] == "passed"
    assert [item["snapshot_id"] for item in result["snapshots"]] == [
        "snapshot-a",
        "snapshot-b",
    ]


def test_quarantine_evidence_must_match_between_backends():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")

    for report in (duckdb, snowflake):
        record = next(
            item
            for item in report["ledger_entries"]
            if item["snapshot_id"] == "snapshot-a"
            and item["source_name"] == "active_permits"
        )
        record.update(
            {
                "quarantined_row_count": 2,
                "quarantined_bad_row_count": 1,
                "quarantined_business_key_count": 1,
                "quarantine_sha256": "c" * 64,
            }
        )

    snowflake_record = next(
        item
        for item in snowflake["ledger_entries"]
        if item["snapshot_id"] == "snapshot-a"
        and item["source_name"] == "active_permits"
    )
    snowflake_record["quarantine_sha256"] = "d" * 64

    with pytest.raises(ValueError, match="Quarantine evidence mismatch"):
        validate_reports(duckdb, snowflake)


def test_failed_native_warehouse_ledger_status_is_rejected():
    duckdb = _report("duckdb")
    duckdb["ledger_entries"][0]["warehouse_status"] = "failed"

    with pytest.raises(ValueError, match="native warehouse ledger status"):
        validate_reports(duckdb, _report("snowflake"))


def test_different_snapshot_coverage_is_rejected():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    snowflake["ledger_entries"] = [
        row for row in snowflake["ledger_entries"] if row["snapshot_id"] != "snapshot-b"
    ]

    with pytest.raises(ValueError, match="coverage differs"):
        validate_reports(duckdb, snowflake)


def test_archive_checksum_mismatch_between_backends_is_rejected():
    with pytest.raises(ValueError, match="provenance mismatch.*archive_sha256"):
        validate_reports(_report("duckdb"), _report("snowflake", checksum="c" * 64))


def test_missing_source_ledger_entry_is_rejected():
    duckdb = _report("duckdb")
    duckdb["ledger_entries"].pop()

    with pytest.raises(ValueError, match="ledger is incomplete"):
        validate_reports(duckdb, _report("snowflake"))


def test_failed_backend_stream_is_rejected():
    snowflake = _report("snowflake")
    snowflake["sources"]["active_permits"]["status"] = "failed"

    with pytest.raises(ValueError, match="active_permits replay stream did not pass"):
        validate_reports(_report("duckdb"), snowflake)


def test_scd2_overlap_is_rejected_even_when_report_claims_passed():
    duckdb = _report("duckdb")
    duckdb["history_validation"]["sources"]["active_permits"][
        "overlapping_intervals"
    ] = 1

    with pytest.raises(ValueError, match="SCD2 integrity violations"):
        validate_reports(duckdb, _report("snowflake"))


def test_unreconciled_history_export_count_is_rejected():
    snowflake = _report("snowflake")
    snowflake["history_validation"]["sources"]["cleared_permits"]["export_rows"] = 9

    with pytest.raises(ValueError, match="do not reconcile"):
        validate_reports(_report("duckdb"), snowflake)


def test_source_identity_mismatch_inside_one_backend_is_rejected():
    duckdb = _report("duckdb")
    duckdb["ledger_entries"][1]["asset_name"] = "different.zip"

    with pytest.raises(ValueError, match="source ledger identity mismatch"):
        validate_reports(duckdb, _report("snowflake"))


def test_pending_or_failed_ledger_entry_is_rejected():
    snowflake = _report("snowflake")
    snowflake["ledger_entries"][0]["status"] = "failed"

    with pytest.raises(ValueError, match="ledger status is not succeeded"):
        validate_reports(_report("duckdb"), snowflake)


def test_backend_history_scope_mismatch_is_rejected():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    snowflake["history_scope"] = "partial"
    snowflake["full_history"] = False
    snowflake["history_scope_basis"] = "explicit_rebuild_selection"
    snowflake["selection"]["limit"] = 1
    snowflake["snapshot_count_selected"] = 1
    snowflake["snapshot_count_pending_before_load"] = 1

    with pytest.raises(ValueError, match="modes or history scopes differ"):
        validate_reports(duckdb, snowflake)


def test_partial_rebuild_scope_is_explicit_and_not_full_history():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    for report in (duckdb, snowflake):
        report["history_scope"] = "partial"
        report["full_history"] = False
        report["history_scope_basis"] = "explicit_rebuild_selection"
        report["selection"]["limit"] = 2
        report["snapshot_count_discovered"] = 4
        report["snapshot_count_selected"] = 2

    result = validate_reports(duckdb, snowflake)
    assert result["history_scope"] == "partial"
    assert result["full_history"] is False
    assert result["explicit_promotion_required"] is True
    assert result["promotion_performed"] is False


def test_backend_selection_parameters_must_match_even_if_scope_matches():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    for report in (duckdb, snowflake):
        report["history_scope"] = "partial"
        report["full_history"] = False
        report["history_scope_basis"] = "explicit_rebuild_selection"
        report["selection"]["limit"] = 2
        report["snapshot_count_discovered"] = 4
        report["snapshot_count_selected"] = 2
    snowflake["selection"]["from_date"] = "2026-11-01"

    with pytest.raises(ValueError, match="modes or history scopes differ"):
        validate_reports(duckdb, snowflake)


def test_full_rebuild_cannot_claim_all_history_when_counts_differ():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    duckdb["snapshot_count_selected"] = 1
    duckdb["snapshot_count_pending_before_load"] = 1

    with pytest.raises(ValueError, match="full rebuild did not select every"):
        validate_reports(duckdb, snowflake)


def test_rebuild_report_cannot_lie_about_full_history_flag():
    duckdb = _report("duckdb")
    duckdb["full_history"] = False

    with pytest.raises(ValueError, match="full_history flag conflicts"):
        validate_reports(duckdb, _report("snowflake"))


def test_scope_validator_rejects_invalid_date_evidence():
    duckdb = _report("duckdb")
    duckdb["selection"]["from_date"] = "2026-02-31"
    duckdb["history_scope"] = "partial"
    duckdb["full_history"] = False
    duckdb["history_scope_basis"] = "explicit_rebuild_selection"

    with pytest.raises(ValueError, match="not a valid calendar date"):
        validate_reports(duckdb, _report("snowflake"))


def test_incremental_scope_is_reported_without_claiming_full_history():
    duckdb = _report("duckdb")
    snowflake = _report("snowflake")
    for report in (duckdb, snowflake):
        report["mode"] = "incremental"
        report["history_scope"] = "incremental"
        report["warehouse_input_staging"] = "required"
        report["full_history"] = False
        report["history_scope_basis"] = "pending_incremental_backlog"
        report["snapshot_count_discovered"] = 1
        report["snapshot_count_selected"] = 1
        report["snapshot_count_pending_before_load"] = 1

    result = validate_reports(duckdb, snowflake)
    assert result["mode"] == "incremental"
    assert result["history_scope"] == "incremental"
    assert result["full_history"] is False
    assert result["explicit_promotion_required"] is True


def test_mode2_rejects_an_extra_input_staging_layer():
    duckdb = _report("duckdb")
    duckdb["warehouse_input_staging"] = "required"

    with pytest.raises(ValueError, match="invalid warehouse input-staging setting"):
        validate_reports(duckdb, _report("snowflake"))
