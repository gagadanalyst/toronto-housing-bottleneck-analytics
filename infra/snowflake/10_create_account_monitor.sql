-- Create the account-level credit monitor.
-- Run as ACCOUNTADMIN. This statement does not assign the monitor.
-- Without FREQUENCY/START_TIMESTAMP, Snowflake uses its legacy
-- calendar-month quota-reset behavior.
-- IF NOT EXISTS does not reconcile the settings of an existing monitor;
-- verify the result before assigning it.

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS TORONTO_ACCOUNT_MONITOR
  WITH CREDIT_QUOTA = 30
  TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;
