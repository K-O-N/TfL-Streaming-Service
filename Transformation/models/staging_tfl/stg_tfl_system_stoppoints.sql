{{ config(materialized='view') }}

with source_stoppoints as (

    select * 
    from {{ source('tfl', 'stoppoints_dim') }}
)
select  cast(naptanId as string) as naptan_id,
        cast(commonName as string) as common_name,
        cast(lat as float64) as latitude,
        cast(lon as float64) as longitude,
        cast(placeType as string) as place_type
       
from source_stoppoints 
QUALIFY ROW_NUMBER() OVER (PARTITION BY naptanId) = 1 