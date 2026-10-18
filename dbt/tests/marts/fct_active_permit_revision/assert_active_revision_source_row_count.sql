-- every fact row represents at least one current source row
SELECT
    permit_num,
    revision_num,
    permit_type,
    current_source_row_count
FROM {{ ref('fct_active_permit_revision') }}
WHERE current_source_row_count IS NULL
   OR current_source_row_count < 1