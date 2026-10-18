SELECT
    permit_num,
    revision_num,
    permit_type,
    COUNT(*) AS row_count
FROM {{ ref('mart_active_permit_age') }}
GROUP BY
    permit_num,
    revision_num,
    permit_type
HAVING COUNT(*) > 1