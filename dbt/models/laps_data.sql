{{ config(materialized='table') }}

select 
	a.meeting_key,
	a.session_key,
	b.driver_id,
	a.i1_speed,
	a.i2_speed,
	a.st_speed,
	a.date_start,
	a.lap_duration,
	a.is_pit_out_lap,
	a.duration_sector_1,
	a.duration_sector_2,
	a.duration_sector_3,
	a.segments_sector_1,
	a.segments_sector_2,
	a.segments_sector_3,
	a.lap_number
from {{ source('dev','laps_data_temp') }} a
join {{ ref('snapshot_dim_driver') }} b
    on a.driver_number = b.driver_number