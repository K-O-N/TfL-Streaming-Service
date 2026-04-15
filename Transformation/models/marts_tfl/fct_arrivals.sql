with line_status as (

    select *  from {{ ref('stg_tfl_system_linestatus') }}

),

arrivals_data as (

    select * from {{ ref('stg_tfl_system_arrivals') }}

)
select  ar.trip_id,
        ar.operation_type,
        ar.vehicle_id,
        ar.station_name,
        ar.platform_name,
        ar.direction,
        ar.destination_name,
        ar.bearing,
        ar.timing_source,
        ar.expected_arrival,
        ar.time_to_station,
        ls.status_severity,
        ls.severity_description,
        ls.reason,
        ls.created_at
        
 from arrivals_data ar 
 inner join line_status ls on ar.line_id = ls.line_id