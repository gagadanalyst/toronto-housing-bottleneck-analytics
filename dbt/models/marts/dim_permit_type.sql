{{ config(materialized='table') }}
select distinct permit_type from
    {{ ref('fct_cleared_permit_revision') }}
