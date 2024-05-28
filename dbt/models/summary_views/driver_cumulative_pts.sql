{{ config(materialized='view') }}
-- current_driver_cumulative_points
with
compiled as (
	select distinct
		b.round,
		b.race_date,
		c.full_name,
		c.team_name,
		a.points,
		b.season
	from {{ ref('race_result') }} a
	left join {{ ref('dim_meeting') }}  b
		on a.meeting_key = b.meeting_key
	left join {{ ref('dim_driver') }} c
		on a.driver_number = c.driver_number
	-- where b.year = (select max(year) from {{ ref('dim_meeting') }})
	order by 1
)
	select
		round,
		race_date,
		full_name,
		team_name,
		points,
		season,
		sum(points) over (partition by full_name,season order by race_date asc) cumulative_points
	from compiled
	order by 1