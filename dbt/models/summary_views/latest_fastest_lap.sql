{{ config(materialized='view') }}
-- Fastest lap result from latest race
select distinct
		c.season,
		c.round,
		b.full_name,
		a.driver_number,
		b.team_name,
		a.fastest_lap_rank,
		a.lap_number,
		a.duration,
		a.speed
	from {{ ref('fastest_lap') }}   a
	join {{ ref('dim_driver') }}  b
		on a.driver_number = b.driver_number
	join {{ ref('dim_meeting') }}  c
		on a.meeting_key = c.meeting_key
	where season = extract(year from current_date)