{{ config(materialized='table') }}

{% set lifecycle_dates_sql %}
    SELECT application_date AS lifecycle_date
    FROM {{ ref('fct_cleared_permit_revision') }}

    UNION ALL

    SELECT issued_date AS lifecycle_date
    FROM {{ ref('fct_cleared_permit_revision') }}

    UNION ALL

    SELECT completed_date AS lifecycle_date
    FROM {{ ref('fct_cleared_permit_revision') }}
{% endset %}

{% set min_date_sql %}
    (
        SELECT MIN(lifecycle_date)
        FROM ({{ lifecycle_dates_sql }}) AS lifecycle_dates
    )
{% endset %}

{% set max_date_sql %}
    (
        SELECT MAX(lifecycle_date)
        FROM ({{ lifecycle_dates_sql }}) AS lifecycle_dates
    )
{% endset %}

{% set end_date_sql = dbt.dateadd('day', 1, max_date_sql) %}

SELECT
    CAST(date_day AS DATE) AS date_day,
    EXTRACT(YEAR FROM date_day) AS year_number,
    EXTRACT(QUARTER FROM date_day) AS quarter_number,
    EXTRACT(MONTH FROM date_day) AS month_number,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    CAST(DATE_TRUNC('month', date_day) AS DATE) AS month_start
FROM (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date=min_date_sql,
        end_date=end_date_sql
    ) }}
) AS date_spine