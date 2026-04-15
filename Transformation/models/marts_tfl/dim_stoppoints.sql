select
      DISTINCT  naptan_id,
      common_name,
      latitude,
      longitude
from {{ ref('int_arrivals_enriched') }}