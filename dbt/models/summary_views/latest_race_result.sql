{{ config(materialized='view') }}
-- latest race result
with
rnk as (
	select distinct
		season,
		round,
		driver_id,
		constructor_id,
		status,
		position,
		positiontext,
		grid,
		points,
		coalesce(relative_time,status) relative_time
	from {{ ref('race_result') }}  a
	where season = extract(year from current_date)
	and round = (select max(round) from {{ ref('race_result') }} where season = extract(year from current_date))
)

select 
	season,
	round,
	concat(given_name,' ',family_name) full_name,
	constructor_name,
	status,
	position,
	positiontext,
	grid,
	points,
	relative_time
from rnk a
left join {{ ref('dim_driver') }}  b
	on a.driver_id = b.driver_id
left join {{ ref('dim_constructor') }} d
	on a.constructor_id = d.constructor_id