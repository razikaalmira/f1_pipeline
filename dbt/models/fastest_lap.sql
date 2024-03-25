{{ config(materialized='table') }}

with
fix_nan as (
	select 
		season,
		round,
		driver_id,
		case when fastestlap = 'nan' then null else fastestlap end as fastestlap
	from {{ source('dev','race_result_temp') }} a
	join {{ ref('snapshot_dim_driver') }} b
		on a.number = cast(b.driver_number as bigint)
),
to_json as (
	select
		season,
		round,
		driver_id,
		fastestlap::jsonb fastest_lap
	from fix_nan
),
extract_col as (
	select distinct
		season,
		round,
		driver_id,
        fastest_lap->>'rank' AS fastest_lap_rank,
        fastest_lap->>'lap' AS lap,
        fastest_lap->'Time'->>'time' AS time,
        fastest_lap->'AverageSpeed'->>'speed' AS average_speed
	from to_json
)
select * from extract_col