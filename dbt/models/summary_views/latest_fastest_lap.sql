{{ config(materialized='view') }}
-- Fastest lap result from latest race
select distinct
		season,
		round,
		concat(given_name,' ',family_name) full_name,
		constructor_name,
		fastest_lap_rank,
		lap,
		time,
		average_speed
	from {{ ref('fastest_lap') }}   a
	join {{ ref('dim_driver') }}  b
		on a.driver_id = b.driver_id
	join {{ ref('dim_constructor') }}  c
		on a.constructor_id = c.constructor_id
	where season = extract(year from current_date)
	and round = (select max(round) from {{ ref('fastest_lap') }} where season = extract(year from current_date))