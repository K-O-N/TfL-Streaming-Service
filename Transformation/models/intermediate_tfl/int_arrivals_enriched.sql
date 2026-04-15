with arrivals as (
    select * from {{ ref('stg_tfl_system_arrivals') }}
), 

stops as (
    select * from {{ ref('stg_tfl_system_stoppoints') }}
),

status as (
    select * from {{ ref('stg_tfl_system_linestatus') }}
)

 select ar.vehicle_id,
        ar.naptan_id,
        ar.station_name,
        ar.line_id,
        ar.line_name,
        ar.platform_name,
        ar.direction,
        ls.status_severity,
        ar.bearing,
        ar.trip_id,
        ar.base_version,
        ar.destination_naptanid,
        ar.destination_name,
        ar.trip_timestamp,
        ar.time_to_station,
        ar.current_location,
        ar.towards,
        ar.expected_arrival,
        ar.time_to_live,
        ar.mode_name,
        st.common_name,
        st.latitude,
        st.longitude 
 from arrivals ar 
 inner join stops st on ar.naptan_id = st.naptan_id 
 inner join status ls on ar.line_id = ls.line_id
