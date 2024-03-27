{{ config(materialized='table') }}

select * from {{ source('dev','weather_info_temp') }}