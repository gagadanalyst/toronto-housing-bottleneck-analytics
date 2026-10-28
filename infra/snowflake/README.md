# Snowflake operations guide

This guide covers the optional Snowflake backend for Toronto Housing Bottleneck Analytics: initial account setup, roles and cost safeguards, automated refreshes, dashboard access, and the procedure for returning to DuckDB.

DuckDB remains independently usable and is the routine CI/CD publication path. Snowflake is a second backend, not a replacement that the rest of the project depends on.

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

The bootstrap uses `ACCOUNTADMIN` for initial provisioning. The ingestion role is intended to write source objects in `RAW`; the dbt role reads raw data and creates models in `ANALYTICS`. The bootstrap SQL does not create or rotate application service users or private keys.

Never commit passwords, account identifiers that are intended to be secret, tokens, or private keys. Use GitHub Actions secrets for workflow authentication and the deployment's secret manager for dashboard credentials. Keep ingestion, dbt, and dashboard service identities separate. The dashboard role should be read-only and must not have access to `TORONTO.RAW`.

## Automated refresh and the Snowflake gate

Routine CI/CD does not require Snowflake credentials. The CI/CD path builds and tests the project, extracts and validates Active and Cleared Permits, builds dbt for DuckDB, and publishes the DuckDB warehouse, Parquet snapshots, and dbt documentation. It also uploads a validated snapshot-bundle artifact with the originating CD run's source files and manifest. Both source configurations use `warehouse_since: 2020-01-01` on `APPLICATION_DATE`. The same dbt project is built for each backend; business rules are not copied into a separate Snowflake model tree.

After an eligible successful CD run on `main`, setting the GitHub repository variable `SNOWFLAKE_OPERATIONS_ENABLED` to the exact string `true` enables the following chain:

1. `snowflake-canonical-raw-refresh.yml` is triggered by the completed CD workflow and uses the snapshot bundle from that exact CD run. It refreshes and verifies canonical `TORONTO.RAW`.
2. After a successful RAW workflow, `snowflake-serving-schema.yml` runs source freshness checks, builds and tests dbt models, and checks the dashboard-facing relations in `TORONTO.ANALYTICS`.

The variable is an **operational gate, not a secret and not the dashboard backend setting**. Six workflows apply it:

- `snowflake-canonical-raw-refresh.yml`
- `snowflake-serving-schema.yml`
- `snowflake-dbt.yml`
- `snowflake-live-ingestion-smoke.yml`
- `snowflake-parity.yml`
- `snowflake.yml`

When the variable is `false` or unset, the guarded jobs skip before their steps connect to Snowflake or run loads/transforms. GitHub may still show a workflow-run record with a `skipped` job; that is expected. The gate does not cancel jobs that are already running, so inspect in-progress runs separately when disabling the feature. Normal CI, CD, DuckDB dbt builds, the rolling release, and GitHub Pages deployment are not disabled by this variable.

Manual Snowflake runs remain available for deliberate validation and maintenance. A manual canonical RAW refresh requires a successful CD run ID and the `confirm_canonical_raw` input. A manual serving build requires `confirm_serving_schema`. The baseline/replay and dbt-baseline workflows require their relevant run inputs. All remain behind the shared variable gate. When the gate is `true`, a parity workflow triggered by a relevant pull request may consume Snowflake compute; ordinary pull-request CI itself does not require Snowflake.

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

The Snowflake serving workflow checks source freshness before building the marts. A successful DuckDB publication does not prove that the Snowflake serving models have refreshed: the dashboard can temporarily lag while the RAW and serving workflows run, or if either workflow fails. For the Snowflake path, check the latest successful canonical RAW refresh and its downstream serving build.

## Configure the Streamlit Snowflake reader

As verified on October 28, 2026, the public deployment selects DuckDB while Snowflake operations are disabled. A local app also uses DuckDB by default when `DASHBOARD_BACKEND` is omitted. The application explicitly reports connection or query errors instead of silently switching to another backend. To deliberately resume Snowflake later, first verify account/trial status and cost safeguards, set `SNOWFLAKE_OPERATIONS_ENABLED=true`, and configure the public app's `DASHBOARD_BACKEND` and read-only secrets separately.

### 1. Grant read-only access

Review `40_dashboard_read_only_role.sql` and execute it deliberately in Snowsight as `ACCOUNTADMIN`. Run it only after the three serving relations exist in `TORONTO.ANALYTICS`.

`TORONTO_DASHBOARD_ROLE` should have:

- `USAGE` on database `TORONTO`, schema `TORONTO.ANALYTICS`, and warehouse `TORONTO_XS_WH`.
- `SELECT` only on these dashboard-facing relations:
  - `TORONTO.ANALYTICS.MART_ACTIVE_PERMIT_AGE`
  - `TORONTO.ANALYTICS.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE`
  - `TORONTO.ANALYTICS.FCT_CLEARED_PERMIT_REVISION`
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

After changing credentials, roles, or serving relations, verify access to all three relations in Snowsight or with an authorized administrator. A deliberate `SELECT COUNT(*)` against each relation is one simple check; it may resume the warehouse and incur compute. Confirm `CURRENT_ROLE()` is `TORONTO_DASHBOARD_ROLE` during the test.

The reader checks that required relations exist, reads dashboard-facing data, and closes the connection after the read. Missing relations, denied permissions, or connection errors are shown to the user; no silent fallback to DuckDB takes place.

## Validation evidence

**Initial account configuration verified on October 25, 2026.** Snowsight checks observed:

- `TORONTO_XS_WH` was X-Small, suspended, configured with 60-second auto-suspend and auto-resume, and assigned `TORONTO_WH_MONITOR`.
- `TORONTO_ACCOUNT_MONITOR` was assigned at account level with a 30-credit monthly quota; `TORONTO_WH_MONITOR` was assigned to the project warehouse with a 20-credit quota.
- Both monitors had notification triggers at 50% and 75%, suspension at 90%, and immediate suspension at 100%. Both showed zero credits used at the time of verification.
- Ingestion and dbt role grants were inspected, including the future-table SELECT grant for RAW.
- Resource-monitor notifications were enabled for the account administrator. `NOTIFY_USERS` was empty, and actual alert delivery was not independently tested.

A fresh `TORONTO.RAW` baseline was loaded from a complete, validated Active/Cleared snapshot. The fresh-baseline replay test confirmed that an unchanged replay added no SCD2 versions and a controlled tracked-attribute change created the expected version ([successful Snapshot Baseline and Replay run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40491637116), added in [test PR #76](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/pull/76)). The legacy DuckDB SCD2 timeline was deliberately not migrated.

The same-snapshot [DuckDB/Snowflake parity run](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40482009588), using `as_of_date=2026-10-27`, reported zero row-value mismatches for compared dimensions, facts, and marts, and matching dashboard summary metrics. Staging comparisons covered schema, grain/count, and semantics rather than full row-by-row values.

After PR #82, the full automatic chain succeeded on commit [`e8bb994`](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/commit/e8bb994c8ec664d2da7d12d1c296a1b15da3dc49) on **October 27, 2026, Toronto time**: [CI](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622105661), [CD/publication](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622186735), [automatic RAW refresh](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40622867433), and [automatic serving build](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/runs/40623059248). These runs verify workflow chaining, not parity between old DuckDB SCD2 history and the intentionally fresh Snowflake baseline.
