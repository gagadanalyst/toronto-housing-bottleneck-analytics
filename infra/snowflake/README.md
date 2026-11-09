# Snowflake operations guide

This guide covers the Snowflake warehouse backend for Toronto Housing Bottleneck Analytics: account setup, roles and cost safeguards, automated refreshes, dashboard access, and the procedure for disabling Snowflake operations.

DuckDB remains the routine CI/CD publication path. Snowflake RAW and serving refreshes are separately gated and do not block DuckDB publication; the dashboard can read from either backend.

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

Routine CI/CD does not require Snowflake credentials. The CI/CD path builds and tests the project, extracts and validates Active and Cleared Permits, builds dbt for DuckDB, and publishes the DuckDB warehouse, Parquet snapshots, and dbt documentation. It also uploads a validated snapshot-bundle artifact with the originating CD run's source files and manifest. Both source configurations use `warehouse_since: 2020-01-01` on `APPLICATION_DATE`. The same dbt project is built for each backend; business rules are not copied into a separate Snowflake model tree.

After an eligible successful CD run on `main`, setting the GitHub repository variable `SNOWFLAKE_OPERATIONS_ENABLED` to the exact string `true` enables the following chain:

1. `cd.yml` sends a `repository_dispatch` containing the exact CD run ID, attempt, source commit, monthly Release tag, and immutable raw-bundle asset name. The RAW workflow replays the approved Release backlog in extraction order; the daily schedule reconciles any missed CD handoff.
2. `snowflake-canonical-raw-refresh.yml` validates the immutable archive backlog and the promoted replay baseline, then clones `TORONTO.RAW` into `TORONTO.RAW_STAGE_<run_id>_<attempt>`. The ingestion role rebases the cloned DLT state, replays pending archives in extraction order, and validates both source ledgers, SCD2 integrity and quarantine reconciliation in the candidate. It swaps the candidate into `RAW` only after all checks pass; the previous RAW remains under the candidate schema name.
3. After RAW promotion succeeds, the workflow explicitly runs `gh workflow run snowflake-serving-schema.yml --ref main` with `confirm_serving_schema=true` and its own `raw_refresh_run_id`. This uses `workflow_dispatch` and the workflow's `actions: write` permission; it does not depend on a `workflow_run` event from a workflow created with `GITHUB_TOKEN`.
4. The serving workflow verifies the supplied RAW run ID through GitHub, creates a fresh `ANALYTICS_STAGE_<run_id>_<attempt>` schema, and points dbt at that candidate. It builds and tests the full project, verifies the four dashboard relations and refresh metadata, grants the dashboard role access only to those relations, and swaps the candidate into `TORONTO.ANALYTICS`. The previous serving schema remains under the candidate name.

The variable is an **operational gate, not a secret and not the dashboard backend setting**. Six workflows apply it:

- `snowflake-canonical-raw-refresh.yml`
- `snowflake-serving-schema.yml`
- `snowflake-dbt.yml`
- `snowflake-live-ingestion-smoke.yml`
- `snowflake-parity.yml`
- `snowflake.yml`

When the variable is `false` or unset, the guarded jobs skip before their steps connect to Snowflake or run loads/transforms. GitHub may still show a workflow-run record with a `skipped` job; that is expected. The gate does not cancel jobs that are already running, so inspect in-progress runs separately when disabling the feature. Normal CI, CD, DuckDB dbt builds, the rolling release, and GitHub Pages deployment are not disabled by this variable.

Manual Snowflake runs remain available for deliberate validation and maintenance. A manual canonical RAW refresh requires a successful CD run ID and the `confirm_canonical_raw` input. A manual serving build requires `confirm_serving_schema`; the automated serving dispatch also supplies `raw_refresh_run_id`, which is verified against the upstream RAW workflow run. The baseline/replay and dbt-baseline workflows require their relevant run inputs. All remain behind the shared variable gate. When the gate is `true`, a parity workflow triggered by a relevant pull request may consume Snowflake compute; ordinary pull-request CI itself does not require Snowflake.

### Staging, quarantine and promotion guarantees

Each Snowflake RAW run works in an isolated schema cloned from production. Both
Active and Cleared are replayed there with the original archive boundary timestamps.
The candidate must have complete per-source ledgers, valid DLT state, no invalid
SCD2 intervals/overlaps/duplicate current keys, and a reconciled quarantine table
before the RAW schema swap. A failed load, validation or threshold check leaves
`TORONTO.RAW` untouched; the candidate schema remains available for inspection.
After the swap, the old RAW schema is retained under the run-specific candidate name.
The DLT state rebase is guarded by a generation marker so a run interrupted after
the swap can finish that state-only recovery on the next attempt.

Serving is built in a separate, empty `ANALYTICS_STAGE_<run_id>_<attempt>` schema.
The dashboard's existing `TORONTO.ANALYTICS` schema remains untouched while dbt
builds, tests, metadata recording and dashboard access checks run. Only then is the
serving schema swapped into place. A failed dbt or pre-promotion validation leaves
the previous serving schema active; the previous version is retained after a swap.
RAW and ANALYTICS are separate atomic schema swaps, not a single Snowflake
transaction. If RAW promotes successfully but serving fails, retry the serving
workflow with the successful RAW run ID; do not replay RAW solely to rebuild dbt.

Quarantine handling is identical in DuckDB and Snowflake. Rows with NULL/blank
business keys and every row belonging to a conflicting duplicate-key group are
excluded from clean SCD2 and retained with the full source record, physical Parquet
row ordinal, row hash, archive identity, Release asset and Parquet checksum.
Redundant identical duplicate occurrences are also retained; the first identical
record is loaded, and redundant occurrences do not count toward the bad-row limit.
For the candidate generation, newly quarantined bad rows are aggregated across
all newly replayed archives for each source independently. Promotion is blocked
when that source total is **at least 25%** of its pre-run production current-row
count. Active and Cleared are checked separately. Quarantine rows already present
in the promoted baseline do not accumulate toward this test.
The full record is still retained in the failed candidate for investigation.
After a successful swap, the queryable table is `TORONTO.RAW.TORONTO_SCD2_QUARANTINE`.
DuckDB also publishes rejected candidate quarantine rows to a separate audit
Release, which is not used by the production recovery restore process. Snowflake
retains the rejected RAW candidate schema. No historical quarantine backfill is
performed; the existing immutable Parquet archives remain the record of prior
snapshots.

A successful Snowflake swap is not coupled transactionally to the DuckDB
publication. The two engines promote independently by design; a failure in one
does not roll back a successful promotion in the other. This is an explicit
independent-backend boundary rather than a distributed transaction guarantee.

For a failed RAW run, a rerun starts from the last promoted RAW schema, creates a
fresh run/attempt candidate, and replays the still-unpromoted immutable archives in
extraction order. A failed candidate is retained for inspection but is never used
as the next production baseline. Stable archive identity, physical row ordinals,
and quarantine IDs make replay deterministic; reprocessing a previously completed
source in a rejected candidate cannot create production duplicates. After successful
RAW promotion, a serving-only retry uses:

```bash
gh workflow run snowflake-serving-schema.yml \
  --ref main \
  -f confirm_serving_schema=true \
  -f raw_refresh_run_id=RAW_REFRESH_RUN_ID
```

### Failed-run recovery checklist

Before retrying, inspect the failed workflow's job steps and identify the last
successful promotion step. A failed workflow run does not by itself prove that
production was unchanged. The RAW and serving promotion scripts attempt rollback
after post-swap verification failures, but a reported rollback failure requires
manual inspection before another write.

| Failure point | Expected production state | Recovery action |
|---|---|---|
| RAW candidate preparation, archive replay, or validation fails before the RAW promotion step succeeds | Existing `TORONTO.RAW` remains active; the rejected run-scoped candidate is not the next baseline | Rerun the failed RAW workflow with a new attempt, or manually dispatch it using an approved CD archive identity as shown below. The new attempt creates a fresh candidate and replays still-pending approved archives in extraction order. |
| RAW swap succeeds but the DLT state rebase is interrupted | The generation marker and retained prior schema identify the in-progress promotion | The next RAW preparation/promotion path can finish the matching state-only rebase when the retained stage schema is present. If logs report a failed automatic rollback, a missing retained schema, or a generation mismatch, stop and inspect `TORONTO.RAW`, the corresponding `RAW_STAGE_<run_id>_<attempt>` schema, its candidate-control marker, and DLT state before retrying. |
| RAW promotion succeeds and the separate serving workflow fails | Promoted `TORONTO.RAW` remains active; the prior `TORONTO.ANALYTICS` remains active until a serving candidate passes | Retry only the serving workflow with the successful RAW run ID. Do not replay RAW solely to rebuild dbt. |
| Serving candidate build or pre-promotion validation fails | Existing `TORONTO.ANALYTICS` remains active | Retry the serving workflow against the same successful RAW run ID. It creates a fresh serving candidate for the new attempt. |
| Serving swap verification or automatic rollback fails | The active and retained serving schemas may need reconciliation | Stop and inspect `TORONTO.ANALYTICS` and the corresponding `ANALYTICS_STAGE_<run_id>_<attempt>` schema before another serving promotion. |

For a pre-promotion RAW failure, rerun the failed refresh job as a new workflow
attempt (which creates a fresh run-scoped candidate):

```bash
gh run rerun RAW_WORKFLOW_RUN_ID --failed \
  --repo gagadanalyst/toronto-housing-bottleneck-analytics
```

Use the manual-dispatch command below when the original automatic handoff was
missed or when you need to select the latest approved archive to reconcile. Do
not use a RAW rerun just to retry serving after RAW has already been promoted.

To manually retry RAW when the automatic repository-dispatch handoff was missed,
select the approved CD snapshot through which the backlog should be reconciled.
The workflow replays incomplete archives up to and including that selected snapshot;
it verifies the Release tag and immutable asset name. Use the CD run ID, its
workflow attempt, and the monthly archive Release tag:

```bash
gh workflow run snowflake-canonical-raw-refresh.yml \
  --ref main \
  -f snapshot_run_id=CD_RUN_ID \
  -f snapshot_run_attempt=CD_RUN_ATTEMPT \
  -f release_tag=toronto-raw-snapshot-archive-YYYY-MM \
  -f confirm_canonical_raw=true
```

If the RAW promotion step succeeded but only the subsequent serving-dispatch step
failed, first verify that the intended generation is now canonical in
`TORONTO.RAW`. The RAW workflow may be marked failed because the dispatch failed,
so the serving workflow's upstream-run verification will reject that run ID. In
that specific case, use the deliberate manual serving path without
`raw_refresh_run_id` (the empty input skips upstream-run verification):

```bash
gh workflow run snowflake-serving-schema.yml \
  --ref main \
  -f confirm_serving_schema=true
```

All manual runs still require `SNOWFLAKE_OPERATIONS_ENABLED=true`; canonical RAW
also requires the canonical replay gate and baseline variables. The two warehouses
remain independent: a successful promotion in one does not roll back the other.

### Disable Snowflake or switch back at trial end

Perform these as separate steps; disabling the workflow gate does not change the dashboard backend.

1. Set the repository variable to `false` with GitHub CLI and verify the saved value:

   ```bash
   gh variable set SNOWFLAKE_OPERATIONS_ENABLED \
     --body "false" \
     --repo gagadanalyst/toronto-housing-bottleneck-analytics
   gh variable get SNOWFLAKE_OPERATIONS_ENABLED \
     --repo gagadanalyst/toronto-housing-bottleneck-analytics
   ```

2. Change the deployed Streamlit setting `DASHBOARD_BACKEND` to `duckdb` (or remove its override to use the code default).
3. Check for Snowflake jobs already in progress; the gate does not cancel them.
4. Run CI on `main` and confirm the subsequent DuckDB CD publication succeeds.
5. Confirm the automatically triggered canonical RAW refresh and serving build are skipped while the gate remains `false`.
6. Open the public dashboard and confirm that it loads data using the published DuckDB release.
7. Once the switch-back is verified and Snowflake is no longer needed, remove its deployment credentials according to the repository's secret-retention policy.

**Gate-off procedure verified on October 28, 2026.** With the variable set to `false`, [manual CI run 40630130221](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40630130221) and [CD run 40630228426](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40630228426) succeeded on commit `e8bb994`. The automatically triggered [canonical RAW refresh 40631433875](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40631433875) and [serving build 40631450406](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40631450406) skipped as intended. The project owner confirmed that the public dashboard loaded successfully after switching its backend to DuckDB.

Do not rely on trial expiry or resource-monitor actions to prevent GitHub Actions from attempting connections. Trial status is account-specific and time-sensitive: check the exact region, edition, remaining balance, and expiry in Snowsight before scheduling final shutdown. Resource monitors are not a substitute for the workflow gate, and the gate does not itself change what backend the dashboard queries.

### Freshness and dashboard lag

The Snowflake serving workflow checks source freshness before building the marts. It records the successful refresh time, source commit, and workflow run in `TORONTO.ANALYTICS.DASHBOARD_REFRESH_METADATA` after its serving checks complete. A successful DuckDB publication does not prove that the Snowflake serving models have refreshed: the dashboard can temporarily lag while the RAW and serving workflows run, or if either workflow fails. For the Snowflake path, check the latest successful canonical RAW refresh and its downstream serving build.

## Configure the Streamlit Snowflake reader

`DASHBOARD_BACKEND` selects the dashboard's data source independently of the Snowflake workflow gate. The app defaults to DuckDB when neither the environment variable nor the Streamlit secret is set; an environment value takes precedence over the secret. Set it to `snowflake` only after configuring the dashboard's read-only role and credentials. Connection or query errors are shown rather than silently switching backends.

### 1. Grant read-only access

Review `40_dashboard_read_only_role.sql` and execute it deliberately in Snowsight as `ACCOUNTADMIN`. Run it only after the four dashboard-facing dbt relations and `DASHBOARD_REFRESH_METADATA` exist in `TORONTO.ANALYTICS`.

`TORONTO_DASHBOARD_ROLE` should have:

- `USAGE` on database `TORONTO`, schema `TORONTO.ANALYTICS`, and warehouse `TORONTO_XS_WH`.
- `SELECT` only on these dashboard-facing relations:
  - `TORONTO.ANALYTICS.MART_ACTIVE_PERMIT_AGE`
  - `TORONTO.ANALYTICS.FCT_ACTIVE_PERMIT_REVISION`
  - `TORONTO.ANALYTICS.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE`
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

After changing credentials, roles, or serving relations, verify access to all four dashboard-facing relations and `DASHBOARD_REFRESH_METADATA` in Snowsight or with an authorized administrator. A deliberate `SELECT COUNT(*)` against each relation is one simple check; it may resume the warehouse and incur compute. Confirm `CURRENT_ROLE()` is `TORONTO_DASHBOARD_ROLE` during the test.

The reader checks that required relations exist, reads dashboard-facing data, and closes the connection after the read. Missing relations, denied permissions, or connection errors are shown to the user; no silent fallback to DuckDB takes place.

## Validation evidence

**Initial account configuration verified on October 25, 2026.** Snowsight checks observed:

- `TORONTO_XS_WH` was X-Small, suspended, configured with 60-second auto-suspend and auto-resume, and assigned `TORONTO_WH_MONITOR`.
- `TORONTO_ACCOUNT_MONITOR` was assigned at account level with a 30-credit monthly quota; `TORONTO_WH_MONITOR` was assigned to the project warehouse with a 20-credit quota.
- Both monitors had notification triggers at 50% and 75%, suspension at 90%, and immediate suspension at 100%. Both showed zero credits used at the time of verification.
- Ingestion and dbt role privileges were inspected. The current candidate promotion path uses explicit schema and table grants; it does not request or depend on a future-table SELECT grant. Any pre-existing future grants are separate account state and are not modified by this workflow.
- Resource-monitor notifications were enabled for the account administrator. `NOTIFY_USERS` was empty, and actual alert delivery was not independently tested.

A fresh `TORONTO.RAW` baseline was loaded from a complete, validated Active/Cleared snapshot. The fresh-baseline replay test confirmed that an unchanged replay added no SCD2 versions and a controlled tracked-attribute change created the expected version ([successful Snapshot Baseline and Replay run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40491637116), added in [test PR #76](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/pull/76)). The legacy DuckDB SCD2 timeline was deliberately not migrated.

The same-snapshot [DuckDB/Snowflake parity run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40482009588), using `as_of_date=2026-10-27`, reported zero row-value mismatches for compared dimensions, facts, and marts, and matching dashboard summary metrics. Staging comparisons covered schema, grain/count, and semantics rather than full row-by-row values.

After PR #82, the full automatic chain succeeded on commit [`e8bb994`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/e8bb994c8ec664d2da7d12d1c296a1b15da3dc49) on **October 27, 2026, Toronto time**: [CI](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622105661), [CD/publication](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622186735), [automatic RAW refresh](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622867433), and [automatic serving build](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40623059248). These runs verify workflow chaining, not parity between old DuckDB SCD2 history and the intentionally fresh Snowflake baseline.

An earlier automated chain succeeded on **November 2, 2026**: [CD #92](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41396374837), [canonical RAW refresh #37](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41396850955), and [serving build #38](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/41397052684) completed successfully on commit `bf27af2`. The serving build passed 52/52 dbt resources and verified all four dashboard relations were non-empty.

The latest verified production chain succeeded on **November 9, 2026**, on merge commit [`6999487`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/69994872b36df99970d734abeaad9b9bd6dc2444): [canonical RAW refresh run 42612648618](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612648618) prepared, replayed, validated, and promoted the isolated RAW candidate, then dispatched the serving build; [serving run 42613324641](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42613324641) verified the upstream RAW run, built and tested dbt, checked dashboard relations/metadata/read access, and promoted the serving candidate. The corresponding [CD run 42612402884](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/42612402884) also succeeded. This is dated run evidence, not a guarantee about later refreshes.
