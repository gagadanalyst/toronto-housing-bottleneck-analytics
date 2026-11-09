# Snapshot replay baseline safety

## Default operating mode

The repository Actions variables below must remain unset or `false` until the
historical rebuild has been completed in isolated targets, DuckDB/Snowflake parity
has passed, and the validated state and complete ledgers have been promoted to the
canonical warehouses with the immutable source archives and repeatable replay path retained:

- `TORONTO_CANONICAL_REPLAY_ENABLED=true`
- `TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID=<run_id>:<attempt>`
- `TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT=<number>`

The baseline ID must identify the final snapshot included in the promoted history.
The count must equal the number of distinct snapshots at or before that baseline
in the promoted ledgers. Both Active and Cleared must be marked `succeeded` for
every snapshot in that range. The runtime guards verify the ID, count, and
per-source completeness; an arbitrary single successful ledger row is insufficient.

While the variables are unset or incomplete:

- CD continues extracting and permanently archiving raw snapshots.
- The independent Snowflake repository-dispatch/scheduled workflow is skipped.
- DuckDB backlog replay is skipped.
- Downstream DuckDB tests, dbt publication, DuckDB release publication, and GitHub
  Pages documentation deployment are skipped, so stale warehouse contents are
  not republished as a fresh dashboard.
- Direct calls to canonical DuckDB and Snowflake replay scripts fail closed.

The archive-retention path remains independent from warehouse mutation. Direct `toronto-ingest` writes to the canonical DuckDB file or Snowflake `RAW` schema are also blocked; use `--extract-only` for extraction and the archive replay loaders for warehouse writes.

## Isolated DuckDB rebuild

Use a database file that is not `data/warehouse/toronto.duckdb`:

```bash
TORONTO_WAREHOUSE_KIND=duckdb \
TORONTO_WAREHOUSE_CREDENTIALS=data/warehouse/toronto-replay-validation.duckdb \
python scripts/load_duckdb_snapshot.py --backlog
```

This is an isolated replay target. Do not publish it as `warehouse-latest` until
the historical history, current-state counts, keys, SCD2 interval overlaps,
quarantines, and parity checks have been reviewed.

## Unified isolated recovery contract

The isolated DuckDB and Snowflake historical replays use the same immutable archive
selection, extraction-time ordering, per-source ledger lifecycle, and canonical dlt
pipeline names (`toronto_active_permits_scd2` and
`toronto_cleared_permits_scd2`). Before either source is written for a snapshot,
both ledger rows are registered as `pending`; each source then transitions through
`loading` to `succeeded` or `failed`. A retry preserves succeeded source rows and
retries only an incomplete/failed source in the same isolated target.

The joint recovery validator checks both 56-entry application ledgers against archive evidence, then validates DLT metadata in both targets. For these loaders, DLT 1.31.0 derives each pipeline's default DLT schema name from its canonical pipeline name; the validator therefore attributes `_dlt_loads.schema_name` to the matching canonical pipeline and requires at least 28 completed loads for each. Completed loads are checked against `_dlt_version` using both `schema_name` and `version_hash`. Existing `_dlt_pipeline_state` checkpoints are decoded with DLT's `decompress_state()` and validated against their pipeline, destination dataset, default schema, schema list, and completed load references. At least one checkpoint per pipeline is required, but a checkpoint is not required for every completed load. The workflow retains joint validation summaries as run artifacts. This validates the recovery process itself, not just equality of the final SCD2 tables.


## Snowflake historical rebuild and promotion

The current canonical backlog loader targets `TORONTO.RAW`. Do not enable it or
point it at production before the baseline contract has been established. Rebuild
Snowflake history in a separate validation schema using the project's controlled
isolated-replay procedure and the same archived Parquet files and extraction-time
boundaries.

Before promotion, verify at minimum:

1. All intended archived snapshots are accounted for in extraction-time order.
2. Both sources are marked succeeded for every completed snapshot in each ledger.
3. Active and Cleared current-state row counts, business keys, and row hashes agree.
4. Full SCD2 validity intervals contain no overlaps or duplicate current keys.
5. DuckDB and Snowflake historical validity timestamps and current state pass the
   agreed parity checks.
6. The immutable approved snapshot archives and unified replay workflow remain available to reconstruct either warehouse; a known-corrupted production copy is not treated as a rollback baseline.

Promotion is a separate controlled operation. This patch does not automate a
Snowflake schema swap or replace canonical warehouse data. The baseline variables
must only be set after the validated database and its complete ledger have been
promoted, and the exact promoted baseline snapshot ID/count have been confirmed.

## Enabling future canonical replay

After promotion, configure all three repository Actions variables with the exact
baseline values. The scripts verify that the canonical DuckDB ledger and
`TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER` exist, that the declared baseline snapshot is
fully complete for both sources, and that the number of snapshots through that
baseline matches the declared count.

Future successful backlog loads will add ledger entries beyond the fixed baseline
and remain ordered by extraction timestamp. If any required variable is removed
or changed to an invalid value, scheduled replay stops; archive retention continues.
