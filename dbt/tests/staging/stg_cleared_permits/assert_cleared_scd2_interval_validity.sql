SELECT
    source_row_version_id,
    source_valid_from,
    source_valid_to
FROM {{ ref('stg_cleared_permits') }}
WHERE
    source_valid_from IS NULL
    OR (
        source_valid_to IS NOT NULL
        AND source_valid_to <= source_valid_from
    )
