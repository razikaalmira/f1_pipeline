{{ config(materialized='table') }}

select distinct
    driver_id,
    driver_number,
    driver_code,
    driver_url,
    given_name,
    family_name,
    birthdate,
    nationality
from {{ ref('snapshot_dim_driver') }}
where dbt_valid_to is null

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
