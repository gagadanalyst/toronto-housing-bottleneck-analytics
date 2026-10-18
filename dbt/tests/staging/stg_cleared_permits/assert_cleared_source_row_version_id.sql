SELECT source_row_version_id
FROM {{ ref('stg_cleared_permits') }}
GROUP BY source_row_version_id
HAVING source_row_version_id IS NULL
OR COUNT(*) > 1