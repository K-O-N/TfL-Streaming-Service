
{{ config(materialized='view') }}

with source_arrivals as (

    select * 
    from {{ source('tfl', 'arrivals_native') }}

)

select  cast(id as string) as trip_id,
        cast(operationType as int) as operation_type,
        cast(vehicleId as string) as vehicle_id,
        cast(naptanId as string) as naptan_id,
        cast(stationName as string) as station_name,
        cast(lineId as string) as line_id,
        cast(lineName as string) as line_name,
        cast(coalesce(platformName, 'other') as string) as platform_name,
        cast(direction as string) as direction,
        cast(bearing as string) as bearing,
        cast(baseVersion as string) as base_version,
        cast(destinationNaptanId as string) as destination_naptanid,
        cast(destinationName as string) as destination_name,
        CAST(timestamp AS TIMESTAMP) AS trip_timestamp,
        cast(timeToStation as int) as time_to_station,
        cast(currentLocation as string) as current_location,
        cast(towards as string) as towards,
        CAST(expectedArrival AS TIMESTAMP) AS expected_arrival,
        CAST(timeToLive AS TIMESTAMP) AS time_to_live,
        cast(modeName as string) as mode_name,
        CAST(timing_source AS TIMESTAMP) AS timing_source,
        CAST(timing_insert AS TIMESTAMP) AS timing_insert,
        CAST(timing_read AS TIMESTAMP) AS timing_read,
        CAST(timing_sent AS TIMESTAMP) AS timing_sent,
        CAST(timing_received AS TIMESTAMP) AS timing_received
from source_arrivals
QUALIFY ROW_NUMBER() OVER ( PARTITION BY id ORDER BY timing_read) = 1