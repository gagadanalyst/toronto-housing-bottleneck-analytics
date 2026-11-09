from __future__ import annotations

from scripts import replay_snowflake_snapshot_backlog as replay


def test_replay_workflow_output_writes_explicit_values(tmp_path, monkeypatch):
    output = tmp_path / "github-output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))

    replay._write_pending_output(False)
    replay._write_pending_output(True)

    assert output.read_text(encoding="utf-8").splitlines() == [
        "has_pending_archives=false",
        "has_pending_archives=true",
    ]


def test_replay_workflow_output_is_optional(monkeypatch):
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    replay._write_pending_output(False)
