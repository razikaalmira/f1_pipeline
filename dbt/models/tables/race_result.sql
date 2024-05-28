{{ config(materialized='table') }}

select
    session_key,
    meeting_key,
    driver_number,
    date as race_date,
    cast(position as int) position,
    case
        when position = '1' then 25
        when position = '2' then 18
        when position = '3' then 15
        when position = '4' then 12
        when position = '5' then 10
        when position = '6' then 8
        when position = '7' then 6
        when position = '8' then 4
        when position = '9' then 2
        when position = '10' then 1
        else 0
    end as points
    -- position,
    -- positiontext,
    -- points,
    -- driver::jsonb->>'driverId' as driver_id,
    -- constructor::jsonb->>'constructorId' as constructor_id,
    -- grid,
    -- laps,
    -- status,
    -- time::jsonb->>'millis' as absolute_millisecond,
    -- time::jsonb->>'time' as relative_time,
    -- season,
    -- round
-- from {{ source('dev','race_result_temp') }}
from {{ source('dev','position_data_temp') }}