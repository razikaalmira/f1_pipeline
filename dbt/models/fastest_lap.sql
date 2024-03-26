{{ config(materialized='table') }}

with
fix_nan as (
	select 
		season,
		round,
		driver::jsonb->>'driverId' driver_id,
		constructor::jsonb->>'constructorId' constructor_id,
		case when fastestlap = 'nan' then null else fastestlap end as fastestlap
	from {{ source('dev','race_result_temp') }}
),
to_json as (
	select
		season,
		round,
		driver_id,
		constructor_id,
		fastestlap::jsonb fastest_lap
	from fix_nan
),
extract_col as (
	select distinct
		season,
		round,
		driver_id,
		constructor_id,
        fastest_lap->>'rank' AS fastest_lap_rank,
        fastest_lap->>'lap' AS lap,
        fastest_lap->'Time'->>'time' AS time,
        fastest_lap->'AverageSpeed'->>'speed' AS average_speed
	from to_json
)
select * from extract_col