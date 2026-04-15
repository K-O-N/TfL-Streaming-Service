with trips as (

    select * from {{ ref('fct_arrivals') }}
)

select
  station_name,
  COUNT(*) AS num_arrivals
from trips
group by  station_name
ORDER BY num_arrivals DESC
LIMIT 10