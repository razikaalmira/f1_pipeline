{{ config(materialized='view') }}
-- current constructor standings
with
joined as (
	select distinct
		m.meeting_key,
		m.season,
		r.driver_number,
		r.points
	from {{ ref('race_result') }} r
	join {{ ref('dim_meeting') }} m
		on r.meeting_key = m.meeting_key
	-- where season = extract(year from current_date)
),
pts as (
select 
	a.season,
		d.team_name,
		sum(a.points) over (partition by d.team_name, a.season) total_points
	from joined a
	join {{ ref('dim_driver') }} d
		on a.driver_number = d.driver_number
)

select distinct *, dense_rank() over (partition by season order by total_points desc) as rank
from pts