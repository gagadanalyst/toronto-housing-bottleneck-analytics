from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.replay_archive import SOURCE_NAMES, validate_warehouse_eligibility

ROOT = Path(__file__).resolve().parents[1]


def _counts(active, cleared=None):
    return {
        "active_permits": active,
        "cleared_permits": active if cleared is None else cleared,
    }


def _archive(
    sequence,
    counts,
    *,
    minimum=1,
    maximum_change=100,
    warnings=None,
    contract=True,
    format_version=2,
    fingerprint_reliable=False,
):
    warnings = warnings or {}
    sources = {
        name: SimpleNamespace(
            row_count=counts[name],
            error=None,
            manifest={
                "quality_warnings": warnings.get(name, []),
                "fingerprint_reliable": fingerprint_reliable,
            },
        )
        for name in SOURCE_NAMES
    }
    bundle = {}
    if contract:
        bundle["replay_contract"] = {
            "source_configuration": {
                "sources": {
                    name: {
                        "min_row_count": minimum,
                        "max_row_change_percent": maximum_change,
                    }
                    for name in SOURCE_NAMES
                }
            }
        }
    return SimpleNamespace(
        order=(sequence,),
        format_version=format_version,
        snapshot_id=f"snapshot-{sequence}",
        sources=sources,
        bundle=bundle,
    )


def test_quality_gate_rejects_below_contract_minimum():
    archive = _archive(1, _counts(5), minimum=10)
    with pytest.raises(ValueError, match="below contract minimum"):
        validate_warehouse_eligibility([archive])


def test_quality_gate_rejects_extraction_quality_warnings():
    archive = _archive(
        1,
        _counts(100),
        warnings={"active_permits": ["below_minimum_row_count"]},
    )
    with pytest.raises(ValueError, match="quality warnings are present"):
        validate_warehouse_eligibility([archive])


def test_quality_gate_rejects_excessive_adjacent_row_change():
    previous = _archive(1, _counts(100), maximum_change=20)
    current = _archive(2, _counts(130), maximum_change=20)
    with pytest.raises(ValueError, match="exceeding the contract maximum"):
        validate_warehouse_eligibility([previous, current])


def test_quality_gate_allows_change_at_exact_contract_limit():
    previous = _archive(1, _counts(100), maximum_change=20)
    current = _archive(
        2,
        _counts(120),
        maximum_change=20,
        fingerprint_reliable=False,
    )
    validate_warehouse_eligibility([previous, current])


def test_quality_gate_uses_v1_archive_as_previous_v2_row_baseline():
    previous = _archive(1, _counts(100), contract=False, format_version=1)
    current = _archive(2, _counts(130), maximum_change=20)
    with pytest.raises(ValueError, match="exceeding the contract maximum"):
        validate_warehouse_eligibility([previous, current])


def test_quality_gate_rejects_zero_row_v2_source():
    archive = _archive(1, _counts(0))
    with pytest.raises(ValueError, match="not warehouse-eligible"):
        validate_warehouse_eligibility([archive])


def test_quality_gate_rejects_invalid_contract_threshold():
    archive = _archive(1, _counts(100), maximum_change=None)
    with pytest.raises(ValueError, match="max_row_change_percent is invalid"):
        validate_warehouse_eligibility([archive])


def test_legacy_v2_without_contract_remains_supported_when_nonempty():
    archive = _archive(1, _counts(100), contract=False)
    validate_warehouse_eligibility([archive])


def test_unreliable_fingerprint_alone_does_not_block_warehouse_replay():
    archive = _archive(1, _counts(100), fingerprint_reliable=False)
    validate_warehouse_eligibility([archive])


def test_full_history_quality_gate_runs_before_candidate_open():
    source = (ROOT / "scripts/replay_snapshots.py").read_text(encoding="utf-8")

    start = source.index("def _validate_full_history_inventory(")
    end = source.index("\ndef run(", start)
    preflight = source[start:end]

    assert "validate_inventory_archives" in preflight
    assert "validate_warehouse_eligibility(archives)" in preflight

    run_body = source[source.index("def run(") :]
    assert run_body.index("_validate_full_history_inventory(args)") < run_body.index(
        "target = _build_target(args)"
    )


def test_v2_manual_publication_requires_explicit_authorization():
    workflow = (ROOT / ".github/workflows/v2-snapshot-archive.yml").read_text(
        encoding="utf-8"
    )

    assert "confirm_publish:" in workflow
    assert "default: false" in workflow
    assert "inputs.confirm_publish == true" in workflow
    assert "vars.TORONTO_EXTRACTION_ENABLED == 'true'" in workflow
    assert "github.ref == 'refs/heads/refactor/separate-extraction'" in workflow


def test_v2_manual_and_scheduled_extraction_have_separate_gates():
    workflow = (ROOT / ".github/workflows/v2-snapshot-archive.yml").read_text(
        encoding="utf-8"
    )

    assert workflow.count("vars.TORONTO_EXTRACTION_SCHEDULE_ENABLED == 'true'") == 6
    assert workflow.count("vars.TORONTO_EXTRACTION_ENABLED == 'true'") == 5
    assert "EXTRACTION_ENABLED: ${{ vars.TORONTO_EXTRACTION_ENABLED }}" in workflow


def test_v2_push_and_pull_request_are_ci_only():
    workflow = (ROOT / ".github/workflows/v2-snapshot-archive.yml").read_text(
        encoding="utf-8"
    )

    assert "  push:" in workflow
    assert "  pull_request:" in workflow
    assert (
        "if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
        in workflow
    )
    assert "github.event_name != 'pull_request' && steps.change_check" not in workflow


def test_snapshot_replay_defaults_to_rebuild_and_cleanup_fails_closed():
    workflow = (ROOT / ".github/workflows/snapshot-replay.yml").read_text(
        encoding="utf-8"
    )

    assert "options: [rebuild, incremental]\n        default: rebuild" in workflow
    assert "steps.replay.outputs.has_pending_archives == 'false'" in workflow
    assert "steps.replay.outputs.has_pending_archives != 'true'" not in workflow
