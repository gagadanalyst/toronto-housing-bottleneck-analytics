# Snowflake foundation

Initial Snowflake configuration for Toronto Housing Bottleneck Analytics.

## Intended configuration

- Database: `TORONTO`
- Source schema: `RAW`
- dbt schema: `ANALYTICS`
- Warehouse: `TORONTO_XS_WH`, X-Small
- Auto-suspend: 60 seconds
- Auto-resume: enabled
- New warehouse initial state: suspended
- Account monitor quota: 30 credits per calendar month
- Project warehouse monitor quota: 20 credits per calendar month
- Ingestion role: `TORONTO_INGEST_ROLE`
- dbt role: `TORONTO_DBT_ROLE`

These are the intended settings. The observed configuration and validation evidence are recorded below.

## Preflight

1. Sign in to Snowsight as `ACCOUNTADMIN`.
2. Run `SHOW RESOURCE MONITORS;` and inspect current assignments.
3. Inspect whether the project database, warehouse, roles, or named monitors
   already exist. If they do, reconcile their settings before continuing.
4. Do not assign the account monitor if an existing account-level monitor
   must be preserved.

## Execution order

Run each file deliberately in Snowsight, then inspect its result:

1. `00_bootstrap.sql` — create database, schemas, roles, warehouse and grants.
2. `10_create_account_monitor.sql` — create the account monitor.
3. Verify that `TORONTO_ACCOUNT_MONITOR` appears with a 30-credit quota.
4. `11_create_warehouse_monitor.sql` — create the project warehouse monitor.
5. Verify that `TORONTO_WH_MONITOR` appears with a 20-credit quota.
6. `20_assign_warehouse_monitor.sql` — attach the project monitor to the
   project warehouse.
7. `30_assign_account_monitor.sql` — only after confirming no existing
   account-level assignment needs to be preserved.
8. `verify_setup.sql` — inspect all objects, assignments, grants and state.

For the monitor creation files, execute the single `CREATE RESOURCE MONITOR`
statement and check the result immediately. `IF NOT EXISTS` does not reconcile
the settings of an existing monitor. If the monitor already exists with
unexpected settings, stop and inspect it instead of blindly replacing it.

If the warehouse state is `STARTED`, explicitly suspend it using
`ALTER WAREHOUSE TORONTO_XS_WH SUSPEND;`, then verify its state again.

## Cost-control limitations

Monitor quotas are measured in credits, not dollars. The account-level monitor
is intended as a backstop for user-managed warehouse compute; the warehouse
monitor applies to the project warehouse.

Resource monitors do not cap all serverless usage or storage costs, and their
actions may have latency. Trial expiry is independent of monitor settings.
Account administrators must enable notifications in Snowsight to receive
monitor alerts.

The bootstrap does not load data or change production CI/CD.

## Security model

The bootstrap uses `ACCOUNTADMIN` for initial provisioning. The ingestion role
is intended to create source objects in `RAW`; the dbt role is intended to
read raw data and create tables and views in `ANALYTICS`.

No service user or private key is created by this task. Service-user
provisioning and key-pair authentication are separate work. Never commit
passwords, tokens, account secrets, or private keys.

## Validation evidence

**Core configuration verified: 2026-10-25.**

Observed in Snowsight:

- Database `TORONTO` and schemas `RAW` and `ANALYTICS` are referenced
  successfully by the verified grants and setup statements.
- `TORONTO_XS_WH` is X-Small and SUSPENDED, with `AUTO_SUSPEND = 60`,
  `AUTO_RESUME = TRUE`, and `TORONTO_WH_MONITOR` assigned.
- `TORONTO_ACCOUNT_MONITOR` is assigned at account level with a 30-credit
  monthly quota.
- `TORONTO_WH_MONITOR` is assigned at warehouse level with a 20-credit
  monthly quota.
- Both monitors have notification triggers at 50% and 75%, suspension at
  90%, and immediate suspension at 100%.
- Ingestion and dbt role grants were inspected. The dbt future-table SELECT
  grant for RAW was also returned by the future-grants query.
- Both monitors reported zero used credits at the time of verification.

**Notification configuration:** Resource-monitor notifications have been
enabled in Snowsight for the account administrator. The `NOTIFY_USERS` column
was empty, so no additional notification recipients were explicitly listed.
Actual alert delivery has not been independently tested.

The initial Snowflake objects and configuration were verified on 2026-10-25.
Since then, the recovered DuckDB history has been migrated to `TORONTO.RAW`,
Snowflake dbt validation has passed, and live Active/Cleared ingestion with
two identical replays has passed in an isolated schema. Analytical parity,
migrated-history continuity, and the production cutover/recovery review remain
separate acceptance steps. These tests have not switched production CI/CD to Snowflake.


## Streamlit dashboard reader

The dashboard can use either the published DuckDB database or the Snowflake
analytics marts. DuckDB remains the default. The public dashboard must not be
switched to Snowflake until its service-user permissions and live read path
have been verified.

### 1. Prepare read-only grants

Review `40_dashboard_read_only_role.sql` and execute it deliberately in
Snowsight as `ACCOUNTADMIN`.

The `TORONTO_DASHBOARD_ROLE` role is intended to have:

- `USAGE` on `TORONTO`, `TORONTO.ANALYTICS`, and `TORONTO_XS_WH`.
- `SELECT` only on the three dashboard-facing relations listed in Step 4.
- No access to `TORONTO.RAW` and no privileges to create, update, or delete
  analytical data.

Run the grants script only after all three relations exist in
`TORONTO.ANALYTICS`. Recheck these object-level grants after dbt rebuilds and
reapply them when necessary. The script does not create a login user or
configure authentication.

### 2. Configure a dedicated dashboard service user

Create or provision a dedicated Snowflake service user using the account's
approved identity and authentication practices. Assign
`TORONTO_DASHBOARD_ROLE` as its default role and configure key-pair
authentication for that user.

Do not reuse the ingestion or dbt service identity merely for convenience.

Keep the private key outside source control. Store it only in the deployment's
secret manager. If the private key is encrypted, store its passphrase as a
separate secret as well.

### 3. Configure Streamlit secrets

The dashboard reader expects the following settings. Add them using the
deployment's secret-management interface, not in a committed file:

```toml
DASHBOARD_BACKEND = "snowflake"

[snowflake]
account = "<your Snowflake account identifier>"
user = "<dedicated dashboard service user>"
private_key = """-----BEGIN PRIVATE KEY-----
<private key contents>
-----END PRIVATE KEY-----"""
warehouse = "TORONTO_XS_WH"
role = "TORONTO_DASHBOARD_ROLE"
database = "TORONTO"
schema = "ANALYTICS"

# Include this only if the key is encrypted:
# private_key_password = "<private key passphrase>"
```

Use the actual account identifier and service-user name provisioned for this
project. Never commit real credentials or private-key material.

`DASHBOARD_BACKEND` can also be set as a deployment environment variable.
That environment value takes precedence over the Streamlit secrets value.

Supported values are `duckdb` and `snowflake`. If the setting is omitted,
the dashboard uses DuckDB.

### 4. Verify access before switching the dashboard

After configuring the service user and role, verify the grants in Snowsight
or with an authorized administrator.

When ready to test the dashboard identity, run read-only queries that confirm
access to all three required relations:

- `TORONTO.ANALYTICS.MART_ACTIVE_PERMIT_AGE`
- `TORONTO.ANALYTICS.MART_CLEARED_LIFECYCLE_BY_YEAR_TYPE`
- `TORONTO.ANALYTICS.FCT_CLEARED_PERMIT_REVISION`

For example, verify that `SELECT COUNT(*)` succeeds against each relation.
These queries can resume the warehouse and incur compute charges, so run
them deliberately. Confirm `CURRENT_ROLE()` is `TORONTO_DASHBOARD_ROLE`
during the test.

The Streamlit reader checks that these relations exist, reads only the
dashboard-facing marts, and closes its connection after the read. A missing
relation, denied permission, or connection error is displayed; the app does
not silently substitute data from DuckDB.

### 5. Switch back to DuckDB

To return to the published DuckDB release, set `DASHBOARD_BACKEND` to
`duckdb`, or remove the setting to use the default.

No dbt transformation or application-code rewrite is required. Confirm the
dashboard can load the published release before considering Snowflake access
retired.

Keep DuckDB as the production default until Snowflake serving has been tested
successfully and the operational switch-back procedure has been verified.
