SELECT *
FROM {{ ref('fct_cleared_permit_revision') }}
WHERE
    permit_num IS NULL OR TRIM(permit_num) = ''
    OR revision_num IS NULL OR TRIM(revision_num) = ''
    OR permit_type IS NULL OR TRIM(permit_type) = ''
