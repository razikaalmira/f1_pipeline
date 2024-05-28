{{ config(materialized='view') }}

with
compiled as (
	select
        b.round,
		b.race_date,
		c.team_name,
		b.season,
		sum(a.points) points
	from {{ ref('race_result') }} a
	join {{ ref('dim_meeting') }}  b
		on a.meeting_key = b.meeting_key
	left join {{ ref('dim_driver') }}  c
		on a.driver_number = c.driver_number
	-- where b.year = (select max(year) from {{ ref('dim_meeting') }})
	group by 1,2,3,4
	order by 1
)
	select
        round,
		race_date,
		team_name,
		points,
		season,
		sum(points) over (partition by team_name,season order by race_date asc) cumulative_points
	from compiled
	order by 1