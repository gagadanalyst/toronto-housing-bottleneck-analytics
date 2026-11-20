"""Tests for the Snapshot Loader Mode 1 Snowflake authorization wrappers."""

from __future__ import annotations

import builtins
import sys
from types import ModuleType

import pytest

from scripts import loader_prepare_snowflake_raw, loader_promote_snowflake_raw

WRAPPERS = [
    pytest.param(
        loader_prepare_snowflake_raw,
        "scripts.prepare_snowflake_raw_candidate",
        id="prepare",
    ),
    pytest.param(
        loader_promote_snowflake_raw,
        "scripts.promote_snowflake_raw_candidate",
        id="promote",
    ),
]


@pytest.mark.parametrize(("wrapper", "legacy_module"), WRAPPERS)
@pytest.mark.parametrize("gate_value", [None, "", "false", "TRUE", "True", "1"])
def test_disabled_gate_blocks_before_legacy_or_connector_import(
    monkeypatch, wrapper, legacy_module, gate_value
):
    if gate_value is None:
        monkeypatch.delenv("SNOWFLAKE_OPERATIONS_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", gate_value)

    attempted_imports = []
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if (
            name == legacy_module
            or name == "snowflake"
            or name.startswith("snowflake.")
        ):
            attempted_imports.append(name)
            raise AssertionError(f"Blocked operation attempted import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    with pytest.raises(PermissionError, match="SNOWFLAKE_OPERATIONS_ENABLED"):
        wrapper.main()

    assert attempted_imports == []


@pytest.mark.parametrize(("wrapper", "legacy_module"), WRAPPERS)
@pytest.mark.parametrize(
    ("legacy_result", "expected_result"),
    [(None, 0), (0, 0), (17, 17)],
)
def test_enabled_gate_delegates_and_propagates_legacy_return_code(
    monkeypatch, wrapper, legacy_module, legacy_result, expected_result
):
    monkeypatch.setenv("SNOWFLAKE_OPERATIONS_ENABLED", "true")

    calls = []
    legacy = ModuleType(legacy_module)

    def legacy_main():
        calls.append("called")
        return legacy_result

    legacy.main = legacy_main
    monkeypatch.setitem(sys.modules, legacy_module, legacy)

    assert wrapper.main() == expected_result
    assert calls == ["called"]
