{{ config(materialized='ephemeral') }}

-- Keep only the latest version of each FAERS report.
-- A report can arrive more than once (follow-up versions or a re-run of the same load date).
with ranked as (
    select
        payload,
        ingested_at,
        row_number() over (
            partition by safetyreportid
            order by
                case when payload ->> 'safetyreportversion' ~ '^\d+$'
                     then (payload ->> 'safetyreportversion')::int end desc nulls last,
                ingested_at desc
        ) as version_rank
    from {{ source('bronze', 'faers_reports') }}
)

select payload, ingested_at
from ranked
where version_rank = 1
