with
compiled as (
	select
        a.round,
		race_date,
		constructor_name,
		sum(points) points
	from {{ ref('race_result') }} a
	left join {{ ref('dim_meeting') }}  b
		on a.season = b.season
		and a.round = b.round
	left join {{ ref('dim_constructor') }} c
		on a.constructor_id = c.constructor_id
	where a.season = (select max(season) from {{ ref('race_result') }})
	group by 1,2,3
	order by 1
)
	select
        round,
		race_date,
		constructor_name,
		points,
		sum(points) over (partition by constructor_name order by race_date asc) cumulative_points
	from compiled
	order by 1