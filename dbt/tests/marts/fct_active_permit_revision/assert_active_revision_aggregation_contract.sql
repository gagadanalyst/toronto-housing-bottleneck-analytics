WITH current_source_rows AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        NULLIF(TRIM(builder_name), '') AS builder_name,
        NULLIF(TRIM(status), '') AS status,
        application_date,
        issued_date,
        geo_id
    FROM {{ ref('stg_active_permits') }}
    WHERE source_valid_to IS NULL
),

expected AS (
    SELECT
        permit_num,
        revision_num,
        permit_type,
        COUNT(*) AS expected_current_source_row_count,

        {{ ordered_distinct_string_agg("COALESCE(builder_name, '<NULL>')", "' | '", "COALESCE(builder_name, '<NULL>')") }}
            AS expected_observed_builder_names,
        CASE
            WHEN COUNT(DISTINCT builder_name) > 1
              OR (
                    COUNT(builder_name) > 0
                    AND COUNT(builder_name) < COUNT(*)
                 )
            THEN TRUE ELSE FALSE
        END AS expected_builder_name_conflict,
        CASE
            WHEN COUNT(builder_name) = COUNT(*)
             AND COUNT(DISTINCT builder_name) = 1
            THEN MIN(builder_name)
        END AS expected_builder_name,

        {{ ordered_distinct_string_agg("COALESCE(status, '<NULL>')", "' | '", "COALESCE(status, '<NULL>')") }}
            AS expected_observed_status_values,
        CASE
            WHEN COUNT(DISTINCT status) > 1
              OR (
                    COUNT(status) > 0
                    AND COUNT(status) < COUNT(*)
                 )
            THEN TRUE ELSE FALSE
        END AS expected_has_status_conflict,
        CASE
            WHEN COUNT(status) = COUNT(*)
             AND COUNT(DISTINCT status) = 1
            THEN MIN(status)
        END AS expected_status,

        {{ ordered_distinct_string_agg("COALESCE(CAST(application_date AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(application_date AS VARCHAR), '<NULL>')") }}
            AS expected_observed_application_dates,
        CASE
            WHEN COUNT(DISTINCT application_date) > 1
              OR (
                    COUNT(application_date) > 0
                    AND COUNT(application_date) < COUNT(*)
                 )
            THEN TRUE ELSE FALSE
        END AS expected_application_date_conflict,
        CASE
            WHEN COUNT(application_date) = COUNT(*)
             AND COUNT(DISTINCT application_date) = 1
            THEN MIN(application_date)
        END AS expected_application_date,

        {{ ordered_distinct_string_agg("COALESCE(CAST(issued_date AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(issued_date AS VARCHAR), '<NULL>')") }}
            AS expected_observed_issued_dates,
        CASE
            WHEN COUNT(DISTINCT issued_date) > 1
              OR (
                    COUNT(issued_date) > 0
                    AND COUNT(issued_date) < COUNT(*)
                 )
            THEN TRUE ELSE FALSE
        END AS expected_issued_date_conflict,
        CASE
            WHEN COUNT(issued_date) = COUNT(*)
             AND COUNT(DISTINCT issued_date) = 1
            THEN MIN(issued_date)
        END AS expected_issued_date,

        {{ ordered_distinct_string_agg("COALESCE(CAST(geo_id AS VARCHAR), '<NULL>')", "' | '", "COALESCE(CAST(geo_id AS VARCHAR), '<NULL>')") }}
            AS expected_observed_geo_ids,
        CASE
            WHEN COUNT(DISTINCT geo_id) > 1
              OR (
                    COUNT(geo_id) > 0
                    AND COUNT(geo_id) < COUNT(*)
                 )
            THEN TRUE ELSE FALSE
        END AS expected_geo_id_conflict,
        CASE
            WHEN COUNT(geo_id) = COUNT(*)
             AND COUNT(DISTINCT geo_id) = 1
            THEN MIN(geo_id)
        END AS expected_geo_id

    FROM current_source_rows
    GROUP BY permit_num, revision_num, permit_type
),

actual AS (
    SELECT *
    FROM {{ ref('fct_active_permit_revision') }}
),

key_coverage_failures AS (
    SELECT
        COALESCE(e.permit_num, a.permit_num) AS permit_num,
        COALESCE(e.revision_num, a.revision_num) AS revision_num,
        COALESCE(e.permit_type, a.permit_type) AS permit_type,
        'fact_key_coverage' AS failed_contract
    FROM expected AS e
    FULL OUTER JOIN actual AS a
        USING (permit_num, revision_num, permit_type)
    WHERE e.expected_current_source_row_count IS NULL
       OR a.current_source_row_count IS NULL
),

contract_failures AS (
    SELECT permit_num, revision_num, permit_type,
           'current_source_row_count' AS failed_contract
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE current_source_row_count
          IS DISTINCT FROM expected_current_source_row_count

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'observed_builder_names'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE observed_builder_names
          IS DISTINCT FROM expected_observed_builder_names

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'builder_name_conflict'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE builder_name_conflict
          IS DISTINCT FROM expected_builder_name_conflict

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'builder_name'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE builder_name IS DISTINCT FROM expected_builder_name

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'observed_status_values'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE observed_status_values
          IS DISTINCT FROM expected_observed_status_values

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'has_status_conflict'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE has_status_conflict
          IS DISTINCT FROM expected_has_status_conflict

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'status'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE status IS DISTINCT FROM expected_status

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'observed_application_dates'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE observed_application_dates
          IS DISTINCT FROM expected_observed_application_dates

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'application_date_conflict'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE application_date_conflict
          IS DISTINCT FROM expected_application_date_conflict

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'application_date'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE application_date IS DISTINCT FROM expected_application_date

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'observed_issued_dates'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE observed_issued_dates
          IS DISTINCT FROM expected_observed_issued_dates

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'issued_date_conflict'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE issued_date_conflict
          IS DISTINCT FROM expected_issued_date_conflict

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'issued_date'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE issued_date IS DISTINCT FROM expected_issued_date

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'observed_geo_ids'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE observed_geo_ids
          IS DISTINCT FROM expected_observed_geo_ids

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'geo_id_conflict'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE geo_id_conflict IS DISTINCT FROM expected_geo_id_conflict

    UNION ALL
    SELECT permit_num, revision_num, permit_type, 'geo_id'
    FROM expected JOIN actual USING (permit_num, revision_num, permit_type)
    WHERE geo_id IS DISTINCT FROM expected_geo_id
)

SELECT * FROM key_coverage_failures
UNION ALL
SELECT * FROM contract_failures