-- Run only after TORONTO_WH_MONITOR exists and its settings are verified.
-- Precondition: TORONTO_XS_WH exists and its current assignments were checked.

USE ROLE ACCOUNTADMIN;

ALTER WAREHOUSE TORONTO_XS_WH
  SET RESOURCE_MONITOR = TORONTO_WH_MONITOR;
