-- checks that conflicting dates and locations are not published, and that the corresponding conflict flags are set.
SELECT *
FROM {{ ref('fct_cleared_permit_revision') }}
WHERE (application_date_conflict
       AND application_date IS NOT NULL)
   OR (issued_date_conflict
       AND issued_date IS NOT NULL)
   OR (completed_date_conflict
       AND completed_date IS NOT NULL)
   OR (geo_id_conflict
       AND geo_id IS NOT NULL)