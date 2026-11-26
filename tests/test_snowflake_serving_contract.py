from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RETIRED_WORKFLOW_PATH = ROOT / "docs/retired-workflows/snowflake-serving-schema.yml"
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
