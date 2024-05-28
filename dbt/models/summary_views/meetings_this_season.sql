{{ config(materialized='view') }}

--get all meetings this season
select
	*
from {{ ref('dim_meeting') }}
where season = (select max(season) from {{ ref('dim_meeting') }})