# Snapshot replay baseline safety

The legacy `promote-scd2-rebuild.yml` workflow is disabled. Production promotion
must use the isolated candidate-validation workflows. If a RAW schema swap has
already completed but its follow-up validation fails, retrying the same candidate
will attempt to restore the previous RAW schema; inspect both schemas if the
automatic rollback itself fails.

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

The historical rebuild described here established the original production
baseline. The current canonical backlog workflow no longer writes directly to
`TORONTO.RAW`: each run clones RAW into a run-specific staging schema, loads and
validates there, then promotes through an atomic schema swap.

Before promotion, verify at minimum:

1. All intended archived snapshots are accounted for in extraction-time order.
2. Both sources are marked succeeded for every completed snapshot in each ledger.
3. Active and Cleared current-state row counts, business keys, and row hashes agree.
4. Full SCD2 validity intervals contain no overlaps or duplicate current keys.
5. DuckDB and Snowflake historical validity timestamps and current state pass the
   agreed parity checks.
6. The immutable approved snapshot archives and unified replay workflow remain available to reconstruct either warehouse; a known-corrupted production copy is not treated as a rollback baseline.

Production promotion is now carried out by the manually dispatched
`promote-scd2-rebuild.yml` workflow with explicit confirmation and exact replay
run inputs. It revalidates the isolated DuckDB/Snowflake recovery, builds and
tests DuckDB dbt, atomically swaps the validated isolated Snowflake schema into
`TORONTO.RAW` (retaining the previous RAW under the isolated schema name), rebases
the DLT pipeline state, and validates promoted RAW against DuckDB and archive
evidence before publishing the recovery checkpoint and four `warehouse-latest`
assets. Set the baseline variables only after this controlled promotion and
serving validation have succeeded.

## Enabling future canonical replay

After promotion, configure all three repository Actions variables with the exact
baseline values. The scripts verify that the canonical DuckDB ledger and
`TORONTO.RAW.TORONTO_SNAPSHOT_LEDGER` exist, that the declared baseline snapshot is
fully complete for both sources, and that the number of snapshots through that
baseline matches the declared count.

## Production recovery and promotion completed

The historical recovery was promoted on November 9, 2026, after the isolated
28-snapshot DuckDB/Snowflake replay, ledger/DLT metadata checks, and exact SCD2
history parity had passed. The canonical baseline remains `42022028625:1` with a
count of 28 snapshots; later production snapshots are appended beyond that fixed
baseline rather than included in its count.

- Historical replay and parity: [run 42547265259](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42547265259).
- Production promotion: [run 42562947635](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42562947635). The isolated Snowflake rebuild was swapped into `TORONTO.RAW`; the previous RAW schema was retained as `TORONTO.SNOWFLAKE_SCD2_REBUILD_20261107_42547265259`. DuckDB recovery checkpoint and all four `warehouse-latest` assets were published and verified.
- First post-recovery CD: [run 42564296816](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42564296816); its independent Snowflake RAW replay passed in [run 42564508188](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42564508188).
- Snowflake serving rebuild after that RAW refresh: [run 42565279093](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42565279093), 52/52 dbt resources passed and the four dashboard-facing relations were verified.

## Serving workflow handoff and retry contract

After a successful canonical RAW refresh, the RAW workflow explicitly dispatches
`snowflake-serving-schema.yml` with `workflow_dispatch`, passing its own run ID.
The serving workflow verifies that run and its RAW refresh job before building
`TORONTO.ANALYTICS`. This avoids relying on a `workflow_run` event after a
`GITHUB_TOKEN`-created workflow run.

For a failed RAW run, rerunning creates a fresh run/attempt candidate cloned from
the last promoted RAW schema, then replays all still-unpromoted archives in
extraction order. A rejected candidate remains available for diagnosis but does
not become the next baseline; production contains neither partial source writes nor
partial ledger updates. Stable archive identity and the original SCD2 boundary make
the replay idempotent. If RAW already passed but serving failed, dispatch serving
again with the successful RAW run ID; do not reload RAW solely to retry dbt.

These are retry guarantees with separate promotion boundaries. Active and Cleared
are loaded into the same RAW candidate; if either source load or candidate
validation fails, canonical `TORONTO.RAW` is not swapped. Successful RAW promotion
is followed by a separate dbt candidate build and ANALYTICS schema swap. A serving
failure leaves the prior `TORONTO.ANALYTICS` active and can be retried without
reloading RAW. DuckDB and Snowflake also promote independently; there is no
distributed transaction across engines.

New row-level quarantine is retained in a dedicated queryable table. NULL/blank
keys and all rows from conflicting duplicate-key groups are excluded from clean
SCD2. Redundant identical duplicate occurrences are retained but do not count as
bad rows. Across the candidate generation, newly quarantined bad rows are
aggregated across all newly replayed archives for each source independently.
Promotion fails when either source total reaches 25% of that source's pre-run
production current-row count. Quarantine rows already present in the promoted
baseline are not counted. Quarantine rows remain in a rejected candidate for
investigation; rejected DuckDB candidates also export an audit-only Release separate
from recovery checkpoints. No historical quarantine backfill is done: the 28
historical Parquet archives remain the source record for that period.

Future successful backlog loads will add ledger entries beyond the fixed baseline
and remain ordered by extraction timestamp. If any required variable is removed
or changed to an invalid value, scheduled replay stops; archive retention continues.
