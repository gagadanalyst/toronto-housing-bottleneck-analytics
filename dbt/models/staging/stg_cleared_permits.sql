-- dbt 2.0.5 strict analysis cannot resolve DuckDB's timezone() function
-- used by the to_utc_date macro; keep baseline until a semantics-safe fix is verified.
{{ config(static_analysis='baseline') }}

with cleaned_typed as (
	select
	_id	as source_uniq_id,
	permit_num,
	revision_num,
	permit_type,
	structure_type,
	work,
	street_num,
	street_name,
	street_type,
	street_direction,
	postal,
	geo_id,
	ward_grid,
	application_date as application_date_raw,
	try_cast(trim(application_date) as date) as application_date,
	issued_date as issued_date_raw,
	try_cast(trim(issued_date) as date) as issued_date,
	completed_date as completed_date_raw,
	-- completed_date is already a timestamp
	-- calendar date encoded at midnight UTC
	{{ to_utc_date('completed_date') }} as completed_date,
	status,
	description,
	current_use,
	proposed_use,
	est_const_cost as est_const_cost_raw,
	try_cast(replace(trim(est_const_cost), ',', '') as double) as est_const_cost,
	assembly,
	institutional,
	residential,
	business_and_personal_services,
	mercantile,
	industrial,
	interior_alterations,
	demolition,
	builder_name,
	dwelling_units_created AS dwelling_units_created_raw,
	try_cast(trim(dwelling_units_created) as BIGINT) as dwelling_units_created,
	dwelling_units_lost AS dwelling_units_lost_raw,
	try_cast(trim(dwelling_units_lost) as BIGINT) as dwelling_units_lost,
	row_hash,
	_dlt_valid_from as source_valid_from,
	_dlt_valid_to as source_valid_to,
	_dlt_load_id as source_load_id,
	_dlt_id as source_row_version_id
from {{ source('toronto_raw', 'cleared_permits') }}
)

select * from cleaned_typed;
