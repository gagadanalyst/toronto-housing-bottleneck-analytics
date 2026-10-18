SELECT
    permit_num,
    revision_num,
    permit_type
FROM {{ ref('stg_active_permits') }}
WHERE NULLIF(TRIM(CAST(permit_num AS VARCHAR)), '') IS NULL
   OR NULLIF(TRIM(CAST(revision_num AS VARCHAR)), '') IS NULL
   OR NULLIF(TRIM(CAST(permit_type AS VARCHAR)), '') IS NULL