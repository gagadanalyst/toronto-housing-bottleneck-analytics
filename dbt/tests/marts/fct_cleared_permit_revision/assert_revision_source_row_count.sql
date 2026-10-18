SELECT *
FROM {{ ref('fct_cleared_permit_revision') }}
WHERE current_source_row_count < 1