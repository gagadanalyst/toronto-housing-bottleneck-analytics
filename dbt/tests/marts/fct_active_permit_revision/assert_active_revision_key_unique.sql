SELECT
    permit_num,
    revision_num,
    permit_type
FROM {{ ref('fct_active_permit_revision') }}
GROUP BY
    permit_num,
    revision_num,
    permit_type
HAVING COUNT(*) > 1