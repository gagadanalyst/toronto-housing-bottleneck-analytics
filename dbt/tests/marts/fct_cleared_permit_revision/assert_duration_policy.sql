SELECT *
FROM {{ ref('fct_cleared_permit_revision') }}
WHERE (
    application_to_issue_days_raw < 0
    AND application_to_issue_days IS NOT NULL
)
OR (
    application_to_issue_days_raw >= 0
    AND application_to_issue_days IS DISTINCT FROM application_to_issue_days_raw
)
OR (
    application_to_completion_days_raw < 0
    AND application_to_completion_days IS NOT NULL
)
OR (
    application_to_completion_days_raw >= 0
    AND application_to_completion_days IS DISTINCT FROM application_to_completion_days_raw
)
OR (
    issue_to_completion_days_raw < 0
    AND issue_to_completion_days IS NOT NULL
)
OR (
    issue_to_completion_days_raw >= 0
    AND issue_to_completion_days IS DISTINCT FROM issue_to_completion_days_raw
)
