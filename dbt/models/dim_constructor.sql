{{ config(materialized='table') }}

with
to_json as (
	select constructor::jsonb constructor
	from {{ source('dev','race_result_temp') }}
),
extract_col as (
	select distinct
			constructor->>'constructorId' AS constructor_id,
			constructor->>'url' AS constructor_url,
			constructor->>'name' AS constructor_name,
			constructor->>'nationality' AS nationality
	from to_json
)
select * from extract_col