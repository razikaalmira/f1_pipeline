{{ config(materialized='view') }}
-- current driver standings
with
rnk as (
	select distinct
		meeting_key,
		driver_number,
		points
		-- sum(points) over (partition by driver_number) total_points
	from {{ ref('race_result') }}
	-- where extract(year from race_date) = extract(year from current_date)
),
final as (
select 
		d.season,
		c.full_name,
	c.driver_number,
		c.team_name,
		c.country_code,
		headshot_url,
		sum(a.points) over (partition by a.driver_number,d.season) total_points
		-- cast(a.total_points as int) as total_points
from rnk a
left join {{ ref('dim_driver') }} c
		on a.driver_number = c.driver_number
	left join {{ ref('dim_meeting') }} d
		on a.meeting_key = d.meeting_key

)

select distinct *,dense_rank() over (partition by season order by total_points desc) as rank
from final