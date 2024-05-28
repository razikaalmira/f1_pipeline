{{ config(materialized='table') }}

with
dis as (
select distinct
    broadcast_name,
    country_code,
    first_name,
    full_name,
    headshot_url,
    last_name,
    driver_number,
    team_colour,
    team_name,
    name_acronym,
    row_number() over (partition by full_name, driver_number order by headshot_url) rn
from {{ ref('snapshot_dim_driver') }}
where dbt_valid_to is null
and team_name is not null
)
select * 
from dis 
where rn = 1
-- with
-- to_json as (
-- 	select driver::jsonb driver
-- 	from {{ source('dev','race_result_temp') }}
-- ),
-- extract_col as (
-- 	select distinct
-- 			driver->>'driverId' AS driver_id,
-- 			driver->>'permanentNumber' AS driver_number,
-- 			driver->>'code' AS driver_code,
-- 			driver->>'url' AS driver_url,
-- 			driver->>'givenName' AS given_name,
-- 			driver->>'familyName' AS family_name,
-- 			driver->>'dateOfBirth' AS birthdate,
-- 			driver->>'nationality' AS nationality
-- 	from to_json
-- )
-- select * from extract_col
-- ;
