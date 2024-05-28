{% snapshot snapshot_dim_driver %}

{{
    config(
      target_database='f1_db',
      target_schema='snapshots',
      unique_key='driver_number',
      strategy='check',
      check_cols=['driver_number']
    )
}}

select
  -- driver_id,
  -- cast(driver_number as int) driver_number,
  -- driver_code,
  -- driver_url,
  -- given_name,
  -- family_name,
  -- birthdate,
  -- nationality
  broadcast_name,
  country_code,
  first_name,
  full_name,
  headshot_url,
  last_name,
  cast(driver_number as int) driver_number,
  team_colour,
  team_name,
  name_acronym
from {{ source('dev','drivers_data_temp') }}

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

{% endsnapshot %}