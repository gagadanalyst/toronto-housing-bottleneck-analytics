from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import yaml

from dashboard.snowflake_backend import REQUIRED_RELATIONS as DASHBOARD_RELATIONS

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github/workflows/snowflake-serving-schema.yml"
PROMOTION_PATH = ROOT / "scripts/promote_snowflake_serving_candidate.py"


def _assignment(source: str, name: str) -> set[str]:
    module = ast.parse(source)
    for node in ast.walk(module):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            value = ast.literal_eval(node.value)
            assert isinstance(value, (set, tuple, list))
            return set(value)
    raise AssertionError(f"Assignment {name!r} not found")


def _workflow_assignment(
    steps: list[dict[str, Any]], step_name: str, assignment_name: str
) -> set[str]:
    step = next(step for step in steps if step.get("name") == step_name)
    match = re.search(
        r"python\s+-\s+<<['\"](?P<marker>[A-Za-z0-9_]+)['\"]\s*\n"
        r"(?P<body>.*?)\n\s*(?P=marker)(?:\s|$)",
        step["run"],
        re.DOTALL,
    )
    assert match is not None, f"Python block missing from {step_name!r}"
    return _assignment(match.group("body"), assignment_name)


def test_snowflake_serving_workflow_covers_the_dashboard_contract() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["build"]["steps"]
    dashboard_relations = set(DASHBOARD_RELATIONS)

    workflow_contracts = (
        ("Validate configuration and inspect target schemas", "expected_models"),
        ("Grant dashboard read-only access to serving candidate", "tables"),
        ("Verify dashboard relations and non-empty results", "required"),
    )
    for step_name, assignment_name in workflow_contracts:
        relations = _workflow_assignment(steps, step_name, assignment_name)
        assert dashboard_relations <= relations

    promoted_relations = _assignment(
        PROMOTION_PATH.read_text(encoding="utf-8"), "REQUIRED_RELATIONS"
    )
    assert dashboard_relations <= promoted_relations
    assert "DASHBOARD_REFRESH_METADATA" in promoted_relations
