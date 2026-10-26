{{ config(materialized='table') }}

-- Include permit types used by either fact.
select permit_type
from {{ ref('fct_active_permit_revision') }}
where permit_type is not null

union

select permit_type
from {{ ref('fct_cleared_permit_revision') }}
where permit_type is not null
