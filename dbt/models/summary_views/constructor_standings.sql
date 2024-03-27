{{ config(materialized='view') }}
-- current constructor standings
with
rnk as (
	select distinct
		season,
		constructor_id,
		sum(points) over (partition by constructor_id,season) total_points
	from {{ ref('race_result') }}
	where season = extract(year from current_date)
)
select 
	a.season,
	d.constructor_name,
	cast(a.total_points as int) total_points,
	dense_rank() over (partition by a.season order by a.total_points desc) as rank
from rnk a
left join {{ ref('dim_constructor') }} d
	on a.constructor_id = d.constructor_id