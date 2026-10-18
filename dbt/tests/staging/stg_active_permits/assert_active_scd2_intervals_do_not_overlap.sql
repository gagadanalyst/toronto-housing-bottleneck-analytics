/*
Why this test matters: the previous test checked whether individual intervals were valid. This one checks whether an earlier version remains valid after a later version has started.

Two versions that meet exactly at a boundary are allowed. Records with the same start time are not compared against each other, so we don't confuse the known simultaneous builder-conflict records with sequential version overlaps.
*/
SELECT
    a.permit_num,
    a.revision_num,
    a.permit_type,
    a.source_row_version_id AS earlier_version_id,
    b.source_row_version_id AS later_version_id,
    a.source_valid_from AS earlier_valid_from,
    a.source_valid_to AS earlier_valid_to,
    b.source_valid_from AS later_valid_from
FROM {{ ref('stg_active_permits') }} a
INNER JOIN {{ ref('stg_active_permits') }} b
    ON a.permit_num = b.permit_num
    AND a.revision_num = b.revision_num
    AND a.permit_type = b.permit_type
    AND a.source_valid_from < b.source_valid_from
WHERE a.source_valid_to IS NULL
    OR b.source_valid_from < a.source_valid_to