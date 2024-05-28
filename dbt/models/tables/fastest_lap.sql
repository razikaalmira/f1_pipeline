{{ config(materialized='table') }}

with
fastest as (
	select
		meeting_key,
		session_key,
		driver_number,
		min(lap_duration) duration
	from {{ source('dev','laps_data_temp') }}
	group by 1,2,3
),
joined as (
select
	f.*,
	lap_number,
	st_speed speed
from fastest f
join {{ source('dev','laps_data_temp') }} l
	on f.meeting_key = l.meeting_key
	and f.session_key = l.session_key
	and f.driver_number = l.driver_number
	and f.duration = l.lap_duration
)

select *, dense_rank() over (partition by meeting_key,session_key order by duration asc) fastest_lap_rank
from joined

-- fix_nan as (
-- 	select 
-- 		season,
-- 		round,
-- 		driver::jsonb->>'driverId' driver_id,
-- 		constructor::jsonb->>'constructorId' constructor_id,
-- 		case when fastestlap = 'nan' then null else fastestlap end as fastestlap
-- 	from {{ source('dev','race_result_temp') }}
-- ),
-- to_json as (
-- 	select
-- 		season,
-- 		round,
-- 		driver_id,
-- 		constructor_id,
-- 		fastestlap::jsonb fastest_lap
-- 	from fix_nan
-- ),
-- extract_col as (
-- 	select distinct
-- 		season,
-- 		round,
-- 		driver_id,
-- 		constructor_id,
--         fastest_lap->>'rank' AS fastest_lap_rank,
--         fastest_lap->>'lap' AS lap,
--         fastest_lap->'Time'->>'time' AS time,
--         fastest_lap->'AverageSpeed'->>'speed' AS average_speed
-- 	from to_json
-- )
-- select * from extract_col