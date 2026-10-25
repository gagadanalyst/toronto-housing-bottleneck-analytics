-- Run only after reviewing SHOW RESOURCE MONITORS.
-- This replaces any currently assigned account-level resource monitor.
-- Do not run if an existing account monitor must be preserved.
-- Confirm that TORONTO_ACCOUNT_MONITOR exists and its settings are verified.

USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT SET RESOURCE_MONITOR = TORONTO_ACCOUNT_MONITOR;
