-- Latest race details
with
filter_date as (
	select distinct season,round
	from {{ ref('race_result') }} 
	where season = extract(year from current_date)
	and round = (select max(round) from {{ ref('race_result') }} where season = extract(year from current_date))
)
	select 
		a.season,
		a.round,
		racename,
		circuit_name,
		circuit_location::jsonb->>'lat' as latitude,
		circuit_location::jsonb->>'long' as longitude,
		date(race_date) race_date
	from {{ ref('dim_meeting') }} a
	join filter_date b
		on a.season = b.season
		and a.round = b.round
