with line_status as (

    select * 
    from {{ ref('stg_tfl_system_linestatus') }}
),
arrivals as (
    select * 
    from {{ ref('stg_tfl_system_arrivals') }}
)
select ar.vehicle_id,
        ar.naptan_id,
        ar.direction,
        ar.destination_name,
        ar.towards,
        ar.station_name,
        ar.platform_name,
        ls.line_id,
        ls.status_severity,
        ls.severity_description,
        ls.reason,
        ls.created_at
from arrivals ar left join line_status ls 
on ar.line_id = ls.line_id 