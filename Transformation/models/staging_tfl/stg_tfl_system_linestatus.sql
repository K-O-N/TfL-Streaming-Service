{{ config(materialized='view') }}

with source_line_status as (

    select * 
    from {{ source('tfl', 'line_status_native') }}
)
select  cast(id as string) as line_id,
        cast(name as string) as line_name,
        cast(modeName as string) as mode_name,
        cast(statusSeverity as int) as status_severity,
        cast(statusSeverityDescription as string) as severity_description,
        cast(reason as string) as reason,
        CAST(created AS TIMESTAMP) AS created_at
from source_line_status 
QUALIFY ROW_NUMBER() OVER (PARTITION BY id order by created DESC) = 1