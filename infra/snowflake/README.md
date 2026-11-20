# Snowflake operations guide

This guide covers the optional Snowflake warehouse backend for Toronto Housing
Bottleneck Analytics: account setup, roles and cost safeguards, Mode 1 incremental
loading, Mode 2 historical reconstruction and promotion, serving-schema access,
and the procedure for disabling Snowflake operations.

DuckDB remains the independent default publication backend. Snowflake Mode 1
loading, full-history replay/promotion, and serving are separately gated by
`SNOWFLAKE_OPERATIONS_ENABLED`; disabling that gate does not disable immutable
source archive publication or the DuckDB Mode 1 path.

## Configuration at a glance

| Setting | Intended value |
|---|---|
| Database | `TORONTO` |
| Source schema | `RAW` |
| dbt serving schema | `ANALYTICS` |
| Warehouse | `TORONTO_XS_WH`, X-Small |
| Auto-suspend / auto-resume | 60 seconds / enabled |
| New warehouse state | Suspended |
| Account monitor | `TORONTO_ACCOUNT_MONITOR`, 30 credits per calendar month |
| Project warehouse monitor | `TORONTO_WH_MONITOR`, 20 credits per calendar month |
| Ingestion role | `TORONTO_INGEST_ROLE` |
| dbt role | `TORONTO_DBT_ROLE` |
| Dashboard role | `TORONTO_DASHBOARD_ROLE`, read-only access to dashboard relations |

These values describe the intended design. The observed state and the date it was checked are recorded in [Validation evidence](#validation-evidence); re-check the account before applying setup SQL. Trial balance and remaining days are time-sensitive, and the exact account region, edition, and expiry are not pinned in this public guide. Verify them in Snowsight before planning a shutdown; a trial may end when either its time limit or credit balance is exhausted.

## Initial account setup

### Preflight

1. Sign in to Snowsight with the necessary administrative role.
2. Run `SHOW RESOURCE MONITORS;` and inspect existing monitors and assignments.
3. Check whether the database, warehouse, roles, and named monitors already exist.
4. Resolve differences before running setup scripts. Do not overwrite an existing account-monitor assignment that must be preserved.

### Execute in order

Run each script deliberately in Snowsight and inspect its output before continuing:

1. `00_bootstrap.sql` — create the database, schemas, roles, warehouse, and base grants.
2. `10_create_account_monitor.sql` — create the account-level resource monitor.
3. Verify `TORONTO_ACCOUNT_MONITOR` and its 30-credit monthly quota.
4. `11_create_warehouse_monitor.sql` — create the project warehouse monitor.
5. Verify `TORONTO_WH_MONITOR` and its 20-credit monthly quota.
6. `20_assign_warehouse_monitor.sql` — assign the project monitor to `TORONTO_XS_WH`.
7. `30_assign_account_monitor.sql` — run only after checking that the account-level assignment can safely be changed.
8. `verify_setup.sql` — inspect objects, grants, monitor assignments, and warehouse state.

The monitor creation files contain a single `CREATE RESOURCE MONITOR` statement. `IF NOT EXISTS` does not reconcile an existing monitor's settings. If a named monitor already exists with unexpected settings, stop and inspect it rather than replacing it blindly.

If the warehouse is `STARTED`, suspend it explicitly:

```sql
ALTER WAREHOUSE TORONTO_XS_WH SUSPEND;
```

Then verify its state. Resource monitors measure credits, not dollars. They do not cap all serverless usage or storage costs, their actions may have latency, and trial expiry is independent of monitor configuration. Notifications must be enabled in Snowsight; successful delivery has not been independently tested. The bootstrap creates the account foundation only—it does not load data or alter production CI/CD.

## Security and credentials

The bootstrap uses `ACCOUNTADMIN` for initial provisioning. The ingestion role owns `RAW` and creates run-specific cloned RAW candidates; the dbt role owns `ANALYTICS` and creates run-specific serving candidates. The dashboard role is read-only. The bootstrap SQL does not create or rotate application service users or private keys.

The automated schema-swap path requires these one-time privileges and owners. Run as `ACCOUNTADMIN` only after checking the current owners and preserving the intended dashboard access:

```sql
USE ROLE ACCOUNTADMIN;

GRANT CREATE SCHEMA ON DATABASE TORONTO TO ROLE TORONTO_DBT_ROLE;
GRANT OWNERSHIP ON SCHEMA TORONTO.ANALYTICS
    TO ROLE TORONTO_DBT_ROLE
    COPY CURRENT GRANTS;
```

`TORONTO_INGEST_ROLE` must continue to own `TORONTO.RAW` and already has `CREATE SCHEMA ON DATABASE TORONTO` from `00_bootstrap.sql`. Do not grant account-level `MANAGE GRANTS`, and do not add future-table grants as part of promotion. The workflows grant the dbt and dashboard roles only the schema/table access needed for the specific candidate.

Never commit passwords, account identifiers that are intended to be secret, tokens, or private keys. Use GitHub Actions secrets for workflow authentication and the deployment's secret manager for dashboard credentials. Keep ingestion, dbt, and dashboard service identities separate. The dashboard role should be read-only and must not have access to `TORONTO.RAW`.

## Automated refresh and the Snowflake gate

The **Mode 1 Incremental Loader (Validated Staging)** workflow (`.github/workflows/mode1-incremental-loader.yml`) is the daily production-loading path. It stages and validates each backend independently before promoting that backend's candidate.

The current operating design separates immutable source extraction, incremental
production loading, historical reconstruction, and explicit promotion.

### Mode 1 incremental Snowflake path

The V2 Snapshot Archive workflow extracts and validates Active and Cleared
snapshots, publishes immutable archive Releases, and updates
`snapshot_inventory.json`. It does not connect to Snowflake or write warehouse
tables.

The Mode 1 loader is `.github/workflows/mode1-incremental-loader.yml`. Its
scheduled run is at 13:00 UTC and requires both
`TORONTO_MODE1_ENABLED=true` and `TORONTO_MODE1_SCHEDULE_ENABLED=true`. A manual
dispatch is restricted to `main` and can select `duckdb`, `snowflake`, or `both`.
The Snowflake job additionally requires `SNOWFLAKE_OPERATIONS_ENABLED=true`.

The Mode 1 Snowflake job:

1. Captures the canonical `TORONTO.RAW` production baseline.
2. Restores or bootstraps its accepted persistent Mode 1 working state.
3. Creates or resumes an isolated run-scoped RAW candidate.
4. Replays only unprocessed approved immutable archives into the candidate.
5. Validates the source ledgers, DLT state, SCD2 integrity, duplicate/current
   keys, quarantine reconciliation, and the captured production baseline.
6. Promotes the validated candidate through the guarded RAW schema swap.
7. Explicitly dispatches the serving workflow with
   `snapshot_loader_run_id` after RAW promotion succeeds.

The persistent Mode 1 state uses the `toronto-mode1-working-state` Release tag.
Snowflake's working schema remains in Snowflake; the corresponding replay ledger
and schema pointer are retained in the state Release. A partial source load can
therefore be retried without treating a failed candidate as production.

The Mode 1 workflow also checks that the retired
`TORONTO_SNAPSHOT_LOADER_ENABLED` variable is not `true`. Keep that legacy
variable false or unset. It is a fail-closed guard, not the switch that enables
the current loader.

### Mode 2 full-history reconstruction and promotion

The manual-only `.github/workflows/snapshot-replay.yml` workflow reconstructs
the selected retained archive history in isolated DuckDB and Snowflake targets.
It does not write to `TORONTO.RAW` or publish a serving schema. The Snowflake
replay and joint comparison require `SNOWFLAKE_OPERATIONS_ENABLED=true`; the
DuckDB replay is independent of that gate.

The separate `.github/workflows/promote-snapshot-rebuild.yml` workflow verifies
the selected successful Mode 2 run, its evidence, and the current production
baselines. It requires explicit
`confirm_full_history_promotion=true`. The DuckDB promotion can proceed
independently; the Snowflake promotion leg is included only when
`SNOWFLAKE_OPERATIONS_ENABLED=true`. After a successful Snowflake promotion,
the workflow explicitly dispatches serving with
`snapshot_rebuild_promotion_run_id`.

### Snowflake serving handoff

`.github/workflows/snowflake-serving-schema.yml` is an explicit serving build.
It accepts at most one verified upstream promotion identity:

- `snapshot_loader_run_id` for successful Mode 1 RAW promotion.
- `snapshot_rebuild_promotion_run_id` for successful Mode 2 full-history
  promotion.

The serving workflow verifies the named successful main-branch workflow run and
the relevant successful promotion job. It then creates a fresh
`ANALYTICS_STAGE_<run_id>_<attempt>` schema, directs dbt to build and test all
models there, verifies the six dashboard relations, refresh metadata and
dashboard read grants, and swaps the candidate into `TORONTO.ANALYTICS`.
It does not depend on a `workflow_run` event from a workflow created with
`GITHUB_TOKEN`.

A deliberate manual serving build is also available with
`confirm_serving_schema=true` and no upstream run ID. Use that path only after
independently verifying the intended RAW generation is already canonical.

### Gate and backend independence

`SNOWFLAKE_OPERATIONS_ENABLED` is an **operational gate, not a secret and not
the dashboard backend setting**. The exact string `true` is required for
Snowflake Mode 1, Snowflake serving, the Snowflake side of Mode 2 replay and
comparison, the Snowflake full-history promotion leg, and the dedicated
Snowflake validation workflows.

When the variable is `false` or unset:

- The Mode 1 DuckDB job can still run when its own Mode 1 controls allow it.
- Snowflake Mode 1 staging and promotion skip.
- Snowflake serving operations skip.
- The Snowflake Mode 2 replay, joint comparison, and promotion leg do not run.
- V2 source extraction and immutable archive publication continue independently.

The gate does not cancel a Snowflake job already running. Inspect in-progress
runs separately when disabling the feature. `DASHBOARD_BACKEND` independently
chooses which backend the Streamlit app reads; the app defaults to DuckDB when
the setting is omitted. Switching the dashboard to DuckDB does not disable
Snowflake workflow jobs, and disabling Snowflake does not automatically change
the dashboard setting.

The normal routine path does not use a separate canonical RAW refresh workflow
or a daily Snowflake archive-reconciliation schedule. Mode 1 is the incremental
production loader; Mode 2 and its promotion workflow are the separate
full-history recovery path.

### Staging, quarantine and promotion guarantees

#### Mode 1 RAW candidate

Each Mode 1 Snowflake run captures the canonical production baseline and works
in an isolated schema cloned from `TORONTO.RAW`. Active and Cleared are replayed
there with their retained archive boundary timestamps. The candidate must have
complete per-source ledgers, valid DLT state, valid SCD2 intervals, no duplicate
current business keys, and reconciled quarantine evidence before the production
baseline is checked again and the RAW schema is swapped.

A failed load, validation, threshold check, or baseline comparison leaves
`TORONTO.RAW` untouched. The rejected stage remains available for inspection.
After a successful swap, the prior RAW generation remains under the
run-specific stage schema name. The DLT state rebase is guarded by a generation
marker so an interrupted post-swap state rebase can be identified and recovered.

#### Mode 2 historical candidate

Mode 2 does not clone the production RAW schema as its replay target. It
reconstructs the selected retained archive history in a separate run-scoped
Snowflake replay schema, preserving evidence and ledger state for validation.
A successful replay is not production promotion. The separate full-history
promotion workflow verifies the selected replay evidence and production
baseline before it replaces canonical RAW. When the Snowflake gate is disabled,
the Snowflake replay/promotion leg is not run.

#### Serving candidate

Serving is built in a separate, empty `ANALYTICS_STAGE_<run_id>_<attempt>`
schema. The dashboard's existing `TORONTO.ANALYTICS` schema remains untouched
while dbt builds and tests, refresh metadata is recorded, and dashboard access
checks run. Only then is the serving schema swapped into place. A failed dbt
build or pre-promotion validation leaves the previous serving schema active;
the previous version is retained after a swap.

RAW and ANALYTICS are separate atomic schema swaps, not one Snowflake
transaction. If RAW promotes successfully but serving fails, retry the serving
workflow with the successful upstream promotion identity; do not replay or
promote RAW solely to rebuild dbt.

#### Quarantine handling

Quarantine handling is consistent across DuckDB and Snowflake. Rows with
NULL/blank business keys and every row belonging to a conflicting
duplicate-key group are excluded from clean SCD2 and retained with the full
source record, physical Parquet row ordinal, row hash, archive identity, Release
asset, and Parquet checksum.

Redundant identical duplicate occurrences are also retained; the first
identical record is loaded, and redundant occurrences do not count toward the
bad-row limit. For a candidate generation, newly quarantined bad rows are
aggregated across all newly replayed archives for each source independently.
Promotion is blocked when that source total is **at least 25%** of its pre-run
production current-row count. Active and Cleared are checked separately.
Quarantine rows already present in the promoted baseline do not accumulate
toward this test.

The full record is retained in a failed candidate for investigation. After a
successful RAW swap, the queryable table is
`TORONTO.RAW.TORONTO_SCD2_QUARANTINE`. DuckDB can publish rejected candidate
quarantine rows to a separate audit Release, which is not used by production
recovery restore. Snowflake retains a rejected RAW candidate schema. No
historical quarantine backfill is performed; immutable snapshot archives
remain the record of earlier captures.

A successful Snowflake swap is not coupled transactionally to DuckDB
publication. The two engines promote independently by design; a failure in one
does not roll back a successful promotion in the other.

### Failed-run recovery checklist

Before retrying, inspect the failed workflow's job steps and identify the last
successful promotion step. A failed workflow run does not by itself prove that
production was unchanged. A reported rollback failure or ambiguous swap outcome
requires manual inspection before another write.

| Failure point | Expected production state | Recovery action |
|---|---|---|
| Mode 1 RAW candidate preparation, replay, or validation fails before promotion | Existing `TORONTO.RAW` remains active; the rejected candidate is not the next baseline | Rerun the failed Mode 1 workflow attempt after resolving the error. The next attempt restores persistent working state and creates or resumes a fresh candidate. |
| Mode 1 RAW promotion succeeds but the serving workflow fails | Promoted `TORONTO.RAW` remains active; the prior `TORONTO.ANALYTICS` remains active until a serving candidate passes | Retry only the serving workflow with the successful Mode 1 run ID. Do not replay RAW solely to rebuild dbt. |
| Mode 2 Snowflake replay or joint parity fails | Canonical `TORONTO.RAW` is unchanged by the isolated replay | Inspect the run's replay summary, ledger, and evidence artifacts; correct the issue and rerun Mode 2. Do not invoke production promotion with incomplete evidence. |
| Mode 2 Snowflake promotion or post-swap recovery fails | The RAW promotion may have reached a separate swap boundary; DuckDB may have a different result | Inspect the promotion run, canonical RAW, retained candidate schema, and generation/state markers before another write. Do not blindly retry an ambiguous promotion. |
| Serving candidate build or pre-promotion validation fails | Existing `TORONTO.ANALYTICS` remains active | Retry serving against the same successful upstream promotion run. A fresh serving candidate is created for the new attempt. |
| One backend succeeds and the other fails | DuckDB and Snowflake promote independently | Recover only the failed backend; there is no distributed transaction or automatic cross-backend rollback. |

For a pre-promotion Mode 1 failure, rerun the failed workflow's unsuccessful
jobs:

```bash
gh run rerun MODE1_RUN_ID --failed --repo gagadanalyst/toronto-housing-bottleneck-analytics
```

A deliberate manual Mode 1 dispatch on `main` can select the backend:

```bash
gh workflow run mode1-incremental-loader.yml --ref main -f backends=snowflake
```

This requires the Mode 1 controls and `SNOWFLAKE_OPERATIONS_ENABLED=true`.
Do not use a manual dispatch to bypass candidate validation or state recovery.

If RAW promotion succeeded and only serving failed, retry serving with the
successful Mode 1 run ID:

```bash
gh workflow run snowflake-serving-schema.yml --ref main -f confirm_serving_schema=true -f snapshot_loader_run_id=MODE1_RUN_ID
```

For a successful Mode 2 full-history promotion, use the promotion run ID instead:

```bash
gh workflow run snowflake-serving-schema.yml --ref main -f confirm_serving_schema=true -f snapshot_rebuild_promotion_run_id=PROMOTION_RUN_ID
```

If a loader/promoter run failed only because its serving-dispatch step failed,
verify that the desired RAW generation is already canonical. In that special
case, a deliberate manual serving build without an upstream ID is available:

```bash
gh workflow run snowflake-serving-schema.yml --ref main -f confirm_serving_schema=true
```

For a Mode 2 replay failure, do not rerun Snowflake production loading. The
replay workflow is manual and isolated; rerunning it does not require a RAW
promotion. A production promotion is a separate action requiring the selected
successful Mode 2 evidence and explicit approval.

### Disable Snowflake or switch back at trial end

Perform these as separate steps; disabling the workflow gate does not change
the dashboard backend.

1. Set the repository variable to `false` with GitHub CLI and verify the saved
   value:

   ```bash
   gh variable set SNOWFLAKE_OPERATIONS_ENABLED --body "false" --repo gagadanalyst/toronto-housing-bottleneck-analytics
   gh variable get SNOWFLAKE_OPERATIONS_ENABLED --repo gagadanalyst/toronto-housing-bottleneck-analytics
   ```

2. Change the deployed Streamlit setting `DASHBOARD_BACKEND` to `duckdb`, or
   remove its override to use the code default.
3. Check for Snowflake jobs already in progress; the gate does not cancel them.
4. Run or verify a successful Mode 1 DuckDB publication.
5. Confirm the Mode 1 Snowflake job, Snowflake serving, and the Snowflake side
   of Mode 2 operations skip while the DuckDB path remains available.
6. Open the public dashboard and confirm that it loads data using the published
   DuckDB release.
7. Once the switch-back is verified and Snowflake is no longer needed, remove
   its deployment credentials according to the repository's secret-retention
   policy.

The V2 archive workflow and DuckDB Mode 1 path are independent of the
Snowflake gate. Do not rely on trial expiry or resource-monitor actions to
prevent GitHub Actions from attempting connections. Trial status is
account-specific and time-sensitive: check the exact region, edition,
remaining balance, and expiry in Snowsight before scheduling final shutdown.
Resource monitors are not a substitute for the workflow gate.

### Freshness and dashboard lag

The Snowflake serving workflow checks source freshness before building the
serving candidate. It records the successful refresh time, source commit, and
workflow run in `TORONTO.ANALYTICS.DASHBOARD_REFRESH_METADATA` as part of the
serving checks. A successful DuckDB publication does not prove that Snowflake
serving has refreshed: the dashboard can temporarily lag while Mode 1 RAW
promotion and the serving workflow run, or if either fails. For the Snowflake
path, check the latest successful Mode 1 RAW promotion or full-history promotion
and its downstream serving build.

## Configure the Streamlit Snowflake reader

`DASHBOARD_BACKEND` selects the dashboard's data source independently of the Snowflake workflow gate. The app defaults to DuckDB when neither the environment variable nor the Streamlit secret is set; an environment value takes precedence over the secret. Set it to `snowflake` only after configuring the dashboard's read-only role and credentials. Connection or query errors are shown rather than silently switching backends.

### 1. Grant read-only access

Review `40_dashboard_read_only_role.sql` and execute it deliberately in Snowsight as `ACCOUNTADMIN`. Run it only after the six dashboard-facing dbt relations and `DASHBOARD_REFRESH_METADATA` exist in `TORONTO.ANALYTICS`.

`TORONTO_DASHBOARD_ROLE` should have:

- `USAGE` on database `TORONTO`, schema `TORONTO.ANALYTICS`, and warehouse `TORONTO_XS_WH`.
- `SELECT` only on these dashboard-facing relations:
  - `TORONTO.ANALYTICS.MART_ACTIVE_PERMIT_AGE`
  - `TORONTO.ANALYTICS.FCT_ACTIVE_PERMIT_REVISION`
  - `TORONTO.ANALYTICS.MART_ACTIVE_BACKLOG_BY_YEAR_TYPE`
  - `TORONTO.ANALYTICS.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE`
  - `TORONTO.ANALYTICS.MART_CLEARED_LIFECYCLE_MATCHED_BY_YEAR_TYPE`
  - `TORONTO.ANALYTICS.FCT_CLEARED_PERMIT_REVISION`
  - `TORONTO.ANALYTICS.DASHBOARD_REFRESH_METADATA` (refresh timestamp only)
- No access to `TORONTO.RAW` and no privileges to create, update, or delete analytical data.

Recheck object-level grants after dbt rebuilds and reapply them if needed. This script does not create a user or configure authentication.

### 2. Use a dedicated dashboard service identity

Create or provision a dedicated service user using the account's approved identity process. Assign `TORONTO_DASHBOARD_ROLE` as its default role and configure key-pair authentication. Do not reuse ingestion or dbt credentials for convenience.

Keep the private key out of source control and store it in the deployment's secret manager. If the key is encrypted, store its passphrase as a separate secret.

### 3. Configure deployment secrets

Use the deployment's Streamlit secrets interface or environment settings, not a committed file. The expected secrets shape is:

```toml
DASHBOARD_BACKEND = "snowflake"

[snowflake]
account = "<Snowflake account identifier>"
user = "<dashboard service user>"
private_key = """-----BEGIN PRIVATE KEY-----
<private key contents>
-----END PRIVATE KEY-----"""
warehouse = "TORONTO_XS_WH"
role = "TORONTO_DASHBOARD_ROLE"
database = "TORONTO"
schema = "ANALYTICS"

# Include only for an encrypted key:
# private_key_password = "<private key passphrase>"
```

`DASHBOARD_BACKEND` can be set either as an environment variable or in Streamlit secrets; the environment value takes precedence. Supported values are `duckdb` and `snowflake`. If omitted, the code defaults to DuckDB.

### 4. Verify dashboard access

After changing credentials, roles, or serving relations, verify access to all six dashboard-facing relations and `DASHBOARD_REFRESH_METADATA` in Snowsight or with an authorized administrator. A deliberate `SELECT COUNT(*)` against each relation is one simple check; it may resume the warehouse and incur compute. Confirm `CURRENT_ROLE()` is `TORONTO_DASHBOARD_ROLE` during the test.

The reader checks that required relations exist, reads dashboard-facing data, and closes the connection after the read. Missing relations, denied permissions, or connection errors are shown to the user; no silent fallback to DuckDB takes place.

## Validation evidence

The account observations and run links below are dated historical evidence.
Several workflow runs predate the Mode 1 / Mode 2 separation and should not be
used as instructions for the current operating path. The current gates,
workflow names, handoffs, and recovery commands are described above.

**Initial account configuration verified on October 25, 2026.** Snowsight checks observed:

- `TORONTO_XS_WH` was X-Small, suspended, configured with 60-second auto-suspend and auto-resume, and assigned `TORONTO_WH_MONITOR`.
- `TORONTO_ACCOUNT_MONITOR` was assigned at account level with a 30-credit monthly quota; `TORONTO_WH_MONITOR` was assigned to the project warehouse with a 20-credit quota.
- Both monitors had notification triggers at 50% and 75%, suspension at 90%, and immediate suspension at 100%. Both showed zero credits used at the time of verification.
- Ingestion and dbt role privileges were inspected. The current candidate promotion path uses explicit schema and table grants; it does not request or depend on a future-table SELECT grant. Any pre-existing future grants are separate account state and are not modified by this workflow.
- Resource-monitor notifications were enabled for the account administrator. `NOTIFY_USERS` was empty, and actual alert delivery was not independently tested.

At the time of the initial Snowflake baseline check, `TORONTO.RAW` was
initialized from a complete, validated Active/Cleared snapshot. The
fresh-baseline replay test confirmed that an unchanged replay added no SCD2
versions and a controlled tracked-attribute change created the expected version
([successful Snapshot Baseline and Replay run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40491637116),
added in [test PR #76](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/pull/76)).
That was an earlier baseline state; the subsequent Mode 2 reconstruction and
promotion established the accepted 28-snapshot archive baseline described in
the [snapshot recovery runbook](../../docs/snapshot-archive-replay-baseline.md).

The same-snapshot [DuckDB/Snowflake parity run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40482009588),
using `as_of_date=2026-10-27`, reported zero row-value mismatches for compared
dimensions, facts, and marts, and matching dashboard summary metrics. Staging
comparisons covered schema, grain/count, and semantics rather than full
row-by-row values. This is historical parity evidence; the later Mode 2
28-snapshot replay and promotion have their own run evidence in the snapshot
recovery runbook.

An earlier automated chain succeeded on commit [`e8bb994`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/e8bb994c8ec664d2da7d12d1c296a1b15da3dc49)
on **October 27, 2026, Toronto time**: [CI](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622105661),
[historical DuckDB publication](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622186735),
[historical Snowflake RAW refresh](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622867433),
and [serving build](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40623059248).
These runs document the earlier workflow chain and do not describe the current
Mode 1 / Mode 2 triggers or handoffs.

An earlier automated chain succeeded on **November 2, 2026**:
[historical DuckDB publication #92](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41396374837),
[historical Snowflake RAW run #37](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41396850955),
and [serving build #38](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41397052684)
completed successfully on commit `bf27af2`. The serving build passed 52/52
dbt resources and verified all four dashboard relations were non-empty. These
are dated pre-separation workflow results.

The **November 9, 2026** production validation on merge commit
[`6999487`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/69994872b36df99970d734abeaad9b9bd6dc2444)
is retained as historical evidence:
[earlier Snowflake RAW run 42612648618](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612648618),
[serving run 42613324641](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42613324641),
and [earlier DuckDB publication run 42612402884](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612402884).
Those runs verified the previous candidate promotion and serving chain. They
remain evidence of those specific executions, not instructions for today's
workflow topology.
