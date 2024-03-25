{{ config(materialized='table') }}

select * from {{ source('dev','session_info_temp') }}