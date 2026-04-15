select
       DISTINCT line_id,
        line_name
        
from {{ ref('int_arrivals_enriched') }}