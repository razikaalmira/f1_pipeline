{{ config(materialized='view') }}
-- current_driver_cumulative_points
with
compiled as (
	select distinct
		a.round,
		race_date,
		concat(given_name,' ',family_name) full_name,
		constructor_name,
		points
	from {{ ref('race_result') }} a
	left join {{ ref('dim_meeting') }}  b
		on a.season = b.season
		and a.round = b.round
	left join {{ ref('dim_driver') }} c
		on a.driver_id = c.driver_id
	left join {{ ref('dim_constructor') }} d
		on a.constructor_id = d.constructor_id
	where a.season = (select max(season) from {{ ref('race_result') }})
	order by 1
)
	select
		round,
		race_date,
		full_name,
		constructor_name,
		points,
		sum(points) over (partition by full_name order by race_date asc) cumulative_points
	from compiled
	order by 1