{{ config(materialized='view') }}
 
with taxi_zone_lookup as 
(
  select *
  from {{ source('staging','taxi_zone_lookup') }}
)
select * from taxi_zone_lookup