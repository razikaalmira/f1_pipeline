{{ config(materialized='view') }}
-- Latest race details

select *
from {{ ref('dim_meeting') }} 
	where season = extract(year from current_date)
	-- and round = (select max(round) from {{ ref('race_result') }} where season = extract(year from current_date))
