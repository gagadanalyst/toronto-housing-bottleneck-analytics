SELECT permit_num, revision_num, permit_type, 'builder_name' AS attribute
FROM {{ ref('fct_active_permit_revision') }}
WHERE builder_name_conflict
  AND builder_name IS NOT NULL

UNION ALL

SELECT permit_num, revision_num, permit_type, 'status' AS attribute
FROM {{ ref('fct_active_permit_revision') }}
WHERE has_status_conflict
  AND status IS NOT NULL

UNION ALL

SELECT permit_num, revision_num, permit_type, 'application_date' AS attribute
FROM {{ ref('fct_active_permit_revision') }}
WHERE application_date_conflict
  AND application_date IS NOT NULL

UNION ALL

SELECT permit_num, revision_num, permit_type, 'issued_date' AS attribute
FROM {{ ref('fct_active_permit_revision') }}
WHERE issued_date_conflict
  AND issued_date IS NOT NULL

UNION ALL

SELECT permit_num, revision_num, permit_type, 'geo_id' AS attribute
FROM {{ ref('fct_active_permit_revision') }}
WHERE geo_id_conflict
  AND geo_id IS NOT NULL