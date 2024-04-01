{{ config(materialized='view') }}
-- current driver standings
with
rnk as (
	select distinct
		season,
		driver_id, 
		constructor_id,
		sum(points) over (partition by driver_id,season) total_points
	from {{ ref('race_result') }}
	where season = extract(year from current_date)
)
select 
	a.season,
	concat(c.given_name,' ',c.family_name) full_name,
	c.driver_number,
	d.constructor_name,
	c.nationality,
	cast(a.total_points as int) as total_points,
	dense_rank() over (partition by a.season order by a.total_points desc) as rank
from rnk a
left join {{ ref('dim_driver') }} c
	on a.driver_id = c.driver_id
left join {{ ref('dim_constructor') }} d
	on a.constructor_id = d.constructor_id