{{ config(materialized='table') }}

with
fix_nan as (
	select
		season,
		round,
		url,
		racename,
		circuit,
		date,
		time,
		firstpractice,
		case when secondpractice = 'nan' then null else secondpractice end as secondpractice,
		case when thirdpractice = 'nan' then null else thirdpractice end as thirdpractice,
		qualifying,
		case when sprint = 'nan' then null else sprint end as sprint
	from {{ source('dev','season_list_temp') }}
),
to_json as (
	select
		season,
		round,
		url,
		racename,
		circuit::jsonb circuit,
		date,
		time,
		firstpractice::jsonb firstpractice,
		secondpractice::jsonb secondpractice,
		thirdpractice::jsonb thirdpractice,
		qualifying::jsonb qualifying,
		sprint::jsonb sprint
	from fix_nan
),
extract_col as (
	select
		season,
		round,
		url,
		racename,
		circuit->>'circuitId' as circuit_id,
		circuit->>'url' as circuit_url,
        circuit->>'circuitName' as circuit_name,
        circuit->>'Location' as circuit_location,
		date race_date,
		time as race_time,
		firstpractice->>'date' as first_practice_date,
        secondpractice->>'date' as second_practice_date,
        thirdpractice->>'date' as third_practice_date,
        qualifying->>'date' as qualifying_date,
        sprint->>'date' as sprint_date
	from to_json
),
meeting_info as (
	select
		meeting_official_name,
		country_key,
		country_code,
		circuit_key,
		circuit_short_name,
		split_part(date_start,'T',1) first_practice_date,
		meeting_key,
		year
	from {{ source('dev','meeting_info_temp') }}
)
select 
	b.meeting_key,
	a.season,
	a.round,
	a.url,
	b.meeting_official_name,
	a.racename,
	a.circuit_id,
	a.circuit_url,
	a.circuit_name,
	a.circuit_location,
	a.race_date,
	a.race_time,
	a.first_practice_date,
	a.second_practice_date,
	a.third_practice_date,
	a.qualifying_date,
	a.sprint_date
from extract_col a
left join meeting_info b
	on a.season = b.year
	and a.first_practice_date = b.first_practice_date