-- Toronto Housing Bottleneck Analytics: core Snowflake objects.
-- Run as ACCOUNTADMIN. This script creates configuration only.
-- Resource monitors are created in separate scripts and verified before use.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS TORONTO
  COMMENT = 'Toronto Housing Bottleneck Analytics warehouse';

CREATE SCHEMA IF NOT EXISTS TORONTO.RAW
  COMMENT = 'dlt-ingested source history';

CREATE SCHEMA IF NOT EXISTS TORONTO.ANALYTICS
  COMMENT = 'dbt analytical models and serving marts';

CREATE ROLE IF NOT EXISTS TORONTO_INGEST_ROLE
  COMMENT = 'Loads Toronto source data through dlt';

CREATE ROLE IF NOT EXISTS TORONTO_DBT_ROLE
  COMMENT = 'Builds and tests Toronto analytics models through dbt';

-- Create the warehouse initially suspended.
-- Its resource monitor is assigned only after the monitor is created
-- and verified in a separate step.
CREATE WAREHOUSE IF NOT EXISTS TORONTO_XS_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Compute for Toronto Housing ingestion and dbt';

-- Reconcile the operational settings on reruns.
-- This does not change the monitor assignment or force a warehouse resume.
ALTER WAREHOUSE TORONTO_XS_WH
  SET WAREHOUSE_SIZE = XSMALL
      AUTO_SUSPEND = 60
      AUTO_RESUME = TRUE
      COMMENT = 'Compute for Toronto Housing ingestion and dbt';

-- Ingestion role: source objects in RAW.
GRANT USAGE ON WAREHOUSE TORONTO_XS_WH
  TO ROLE TORONTO_INGEST_ROLE;

GRANT USAGE ON DATABASE TORONTO
  TO ROLE TORONTO_INGEST_ROLE;

GRANT USAGE ON SCHEMA TORONTO.RAW
  TO ROLE TORONTO_INGEST_ROLE;

GRANT CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA TORONTO.RAW
  TO ROLE TORONTO_INGEST_ROLE;

-- dbt role: read raw data and create analytics objects.
GRANT USAGE ON WAREHOUSE TORONTO_XS_WH
  TO ROLE TORONTO_DBT_ROLE;

GRANT USAGE ON DATABASE TORONTO
  TO ROLE TORONTO_DBT_ROLE;

GRANT USAGE ON SCHEMA TORONTO.RAW
  TO ROLE TORONTO_DBT_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA TORONTO.RAW
  TO ROLE TORONTO_DBT_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA TORONTO.RAW
  TO ROLE TORONTO_DBT_ROLE;

GRANT USAGE ON SCHEMA TORONTO.ANALYTICS
  TO ROLE TORONTO_DBT_ROLE;

GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA TORONTO.ANALYTICS
  TO ROLE TORONTO_DBT_ROLE;
