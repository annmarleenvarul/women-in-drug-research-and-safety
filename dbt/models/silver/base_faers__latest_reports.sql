{{ config(materialized='ephemeral') }}

-- Keep only the latest version of each FAERS report.
-- A report can arrive more than once (follow-up versions; identical ID/version reruns are ignored in Raw).
with ranked as (
    select
        payload,
        loaded_at as ingested_at,
        row_number() over (
            partition by report_id
            order by
                case when report_version ~ '^\d+$'
                     then (report_version)::numeric end desc nulls last,
                loaded_at desc,
                report_version desc
        ) as version_rank
    from {{ source('raw', 'faers_reports') }}
)

select payload, ingested_at
from ranked
where version_rank = 1
