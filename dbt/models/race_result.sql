{{ config(materialized='table') }}
select
    position,
    positiontext,
    points,
    driver::jsonb->>'driverId' as driver_id,
    constructor::jsonb->>'constructorId' as constructor_id,
    grid,
    laps,
    status,
    time::jsonb->>'millis' as absolute_millisecond,
    time::jsonb->>'time' as relative_time,
    season,
    round
from {{ source('dev','race_result_temp') }}