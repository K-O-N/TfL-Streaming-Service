with delayed_lines as (
    select
        *
    from {{ ref('int_line_health') }}
    where status_severity = 0
)

select 
    distinct date_trunc(created_at, DAY) as delay_hour,
    line_id
from delayed_lines

