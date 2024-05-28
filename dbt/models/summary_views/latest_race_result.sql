{{ config(materialized='view') }}
-- latest race result
	select distinct
	b.season,
	b.round,
	a.driver_number,
	c.team_name,
	a.position,
	a.points,
	c.full_name
from {{ ref('race_result') }} a
join {{ ref('dim_meeting') }} b
	on a.meeting_key = b.meeting_key
join {{ ref('dim_driver') }} c
	on a.driver_number = c.driver_number
where b.season = extract(year from current_date)
	-- and round = (select max(round) from {{ ref('race_result') }} where season = extract(year from current_date))
