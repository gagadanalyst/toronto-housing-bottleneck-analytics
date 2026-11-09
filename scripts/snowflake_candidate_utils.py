"""Shared helpers for isolated Snowflake RAW candidate creation and cutover."""

from __future__ import annotations

from dlt.common.versioned_state import (
    bump_state_version_if_modified,
    compress_state,
    decompress_state,
)

PIPELINES = {
    "toronto_active_permits_scd2",
    "toronto_cleared_permits_scd2",
}


def decode_state(value, pipeline: str) -> dict:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = decompress_state(value)
    if not isinstance(value, dict) or not value:
        raise ValueError(f"Invalid DLT state payload for {pipeline}.")
    return value


def rebase_pipeline_state(
    cursor, schema: str, source_dataset: str, target_dataset: str
) -> int:
    """Rebase cloned DLT dataset state while preserving its version/hash contract."""
    cursor.execute(
        f'SELECT PIPELINE_NAME, STATE, _DLT_LOAD_ID FROM TORONTO."{schema}"._DLT_PIPELINE_STATE'
    )
    rows = cursor.fetchall()
    seen: set[str] = set()
    updates = 0
    for pipeline_value, state_value, load_id in rows:
        pipeline = str(pipeline_value).lower()
        if pipeline not in PIPELINES:
            raise ValueError(f"Unexpected DLT pipeline state row: {pipeline_value!r}.")
        seen.add(pipeline)
        payload = decode_state(state_value, pipeline)
        if str(payload.get("pipeline_name", "")).lower() != pipeline:
            raise ValueError(f"DLT pipeline state name mismatch for {pipeline}.")
        dataset = str(payload.get("dataset_name", "")).upper()
        if dataset == target_dataset.upper():
            continue
        if dataset != source_dataset.upper():
            raise ValueError(
                f"DLT state for {pipeline} points to {dataset!r}; expected "
                f"{source_dataset!r} or {target_dataset!r}."
            )
        payload["dataset_name"] = target_dataset.upper()
        _version, new_hash, _old_hash = bump_state_version_if_modified(payload)
        cursor.execute(
            f'''UPDATE TORONTO."{schema}"._DLT_PIPELINE_STATE
                SET STATE = %s, VERSION = %s, ENGINE_VERSION = %s, VERSION_HASH = %s
                WHERE PIPELINE_NAME = %s AND _DLT_LOAD_ID = %s''',
            (
                compress_state(payload),
                int(payload["_state_version"]),
                int(payload["_state_engine_version"]),
                new_hash,
                pipeline_value,
                load_id,
            ),
        )
        if int(cursor.rowcount) != 1:
            raise RuntimeError(
                f"Expected one DLT state update for {pipeline}/{load_id}; "
                f"updated {cursor.rowcount}."
            )
        updates += 1
    missing = PIPELINES - seen
    if missing:
        raise ValueError(
            f"Candidate DLT pipeline state is incomplete: {sorted(missing)}."
        )
    return updates
