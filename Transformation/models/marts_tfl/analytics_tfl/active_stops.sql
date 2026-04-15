with trips as (

    select * from {{ ref('int_arrivals_enriched') }}
)
select
  naptan_id,
  COUNT(*) AS num_arrivals
from trips
group by naptan_id
order by num_arrivals DESC
limit 10
