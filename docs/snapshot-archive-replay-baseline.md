# Snapshot replay baseline safety

The legacy `promote-scd2-rebuild.yml` workflow is disabled. The one-time
historical baseline promotion is complete; routine production refreshes must use
the current isolated DuckDB CD and Snowflake RAW/serving candidate workflows. A
post-swap validation failure triggers an automatic rollback attempt. If the workflow
reports that rollback failed, do not blindly retry: inspect `TORONTO.RAW` and the
retained run-specific RAW stage schema before any further promotion.

## Default operating mode

The historical rebuild has now been completed in isolated targets, DuckDB/Snowflake
parity passed, and the validated history and complete ledgers were promoted to the
canonical warehouses. The fixed promoted baseline is `42022028625:1`, containing
28 distinct snapshots. Canonical backlog replay uses these baseline controls:

- `TORONTO_CANONICAL_REPLAY_ENABLED=true`
- `TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_ID=42022028625:1`
- `TORONTO_CANONICAL_REPLAY_BASELINE_SNAPSHOT_COUNT=28`

Keep the baseline ID and count fixed: they identify the final snapshot in the
historical baseline, not the moving total of later production snapshots. Both
Active and Cleared must be marked `succeeded` for every snapshot through that
baseline. Runtime guards verify the ID, count, and per-source completeness; an
arbitrary single successful ledger row is insufficient.

If any canonical replay variable is unset, incomplete, or invalid, the workflows
fail closed for warehouse replay. The separate `SNOWFLAKE_OPERATIONS_ENABLED`
variable controls whether Snowflake compute is permitted; it does not replace the
canonical replay baseline controls.

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

The one-time historical production promotion completed on November 9, 2026
([promotion run 42562947635](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42562947635)).
That historical rebuild workflow is now disabled; it is not the routine production
refresh path. Routine DuckDB publication and Snowflake RAW/serving promotion use
the current run-scoped candidate workflows described in the [Snowflake operations
guide](../infra/snowflake/README.md). The fixed baseline variables above were
activated only after the historical promotion and validation completed.

## Canonical replay baseline validation

The canonical replay baseline controls were configured after the
November 9 historical promotion. The scripts verify that the canonical
DuckDB ledger and Snowflake snapshot ledger exist, that the declared
baseline snapshot is complete for both sources, and that the number
of snapshots through that baseline matches the configured count.

Keep these values fixed for routine replay. Changing the historical
baseline requires a separately validated recovery and promotion.

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
- Post-PR #107 reliability verification on merge commit [`6999487`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/69994872b36df99970d734abeaad9b9bd6dc2444): [CD / DuckDB publication run 42612402884](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612402884), [Snowflake RAW run 42612648618](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612648618), and [Snowflake serving run 42613324641](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42613324641) all succeeded. CD published the validated DuckDB recovery checkpoint and rolling assets; RAW and serving each passed isolated candidate validation and promotion.

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

## Routine failure recovery

Use the last successful promotion boundary, not just the overall workflow conclusion,
to choose the recovery action. The detailed Snowflake commands and swap-recovery
checks are in the [Snowflake operations guide](../infra/snowflake/README.md).

| Failure point | Production boundary | Recovery guidance |
|---|---|---|
| CD extraction or snapshot approval fails | Warehouse loading is gated until both source snapshots are complete and approved; the raw evidence archive is independent | Rerun an eligible CD attempt after resolving the source/API issue. Do not load an incomplete or unapproved bundle. |
| DuckDB candidate replay, SCD2/quarantine validation, or dbt fails before candidate promotion | The last immutable DuckDB recovery checkpoint remains the restore point | Rerun the failed CD data-publication job. The next attempt restores the latest verified checkpoint, creates a fresh candidate, and replays the approved archive backlog. Do not promote a failed runner-local candidate. |
| DuckDB recovery-checkpoint publication fails before a complete checkpoint is verified | The latest checkpoint that passes integrity checks remains the restore point | Rerun the failed CD data-publication job. The next attempt restores the last verified checkpoint and replays the approved immutable archive backlog. Inspect Release assets if publication was partial; do not manually use an incomplete asset pair. |
| DuckDB rolling `warehouse-latest` publication fails after the new recovery checkpoint was published | The validated immutable recovery checkpoint has already advanced | A later CD attempt restores that checkpoint and retries publication without replaying from an older warehouse state. If the publisher reports that rollback also failed, inspect its recovery backup artifact and Release assets before another publication attempt. |
| Snowflake RAW candidate fails before promotion | Existing canonical `TORONTO.RAW` remains active; the rejected stage is not used as the next baseline | Retry RAW with a fresh workflow attempt so it clones the last promoted RAW and replays still-pending approved archives in extraction order. Do not reuse a stale candidate. |
| Snowflake RAW fails during/after swap or reports rollback failure | A schema swap may have occurred; the old RAW schema is retained under the run-specific stage name when the swap succeeds | Check the RAW promotion step, candidate-generation marker, canonical DLT state, and both schemas. The automatic recovery path can finish a matching interrupted DLT rebase when the retained schema exists. If rollback failed or the state is ambiguous, stop and inspect before retrying. |
| Snowflake serving fails after RAW succeeds | The successful RAW promotion remains active; the prior `TORONTO.ANALYTICS` remains active until serving promotion | Retry only the serving workflow with the successful RAW run ID. Do not reload RAW merely to retry dbt. If RAW promotion succeeded but the serving-dispatch step failed, verify canonical RAW and use the deliberate manual serving dispatch without a RAW run ID, as described in the Snowflake guide. |
| One backend succeeds and the other fails | DuckDB and Snowflake promote independently | Recover only the failed backend; there is no distributed transaction and no automatic cross-backend rollback. |

A rejected DuckDB quarantine-audit Release is evidence for investigation, not a
recovery checkpoint. Restore only the verified immutable DuckDB recovery checkpoint;
never substitute the quarantine-audit database or a rejected candidate.

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
