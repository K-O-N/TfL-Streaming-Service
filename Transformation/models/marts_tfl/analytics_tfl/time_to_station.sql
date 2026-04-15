with fct_arrivals as (

    select * from {{ ref('fct_arrivals') }}
)
select
  trip_id,
  AVG(time_to_station) AS avg_time_to_station_seconds,
  AVG(TIMESTAMP_DIFF(expected_arrival, timing_source, MINUTE)) AS avg_time_to_station
from fct_arrivals
group by trip_id
order by avg_time_to_station_seconds desc
limit 10