{{ config(materialized='view') }}

--get all meetings this season
select
	season,
	round,
	racename,
	circuit_name,
	circuit_location::jsonb->>'country' country,
	circuit_location::jsonb->>'locality' locality,
	circuit_location::jsonb->>'lat' latitude,
	circuit_location::jsonb->>'long' longitude,
	race_date
from {{ ref('dim_meeting') }}
where season = (select max(season) from {{ ref('dim_meeting') }})