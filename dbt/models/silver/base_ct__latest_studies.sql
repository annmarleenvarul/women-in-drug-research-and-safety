{{ config(materialized='ephemeral') }}

-- Keep only the most recently ingested copy of each study.
-- Earlier states of a study are preserved by the snapshot (SCD Type 2), not here.
with ranked as (
    select
        payload -> 'protocolSection' as protocol,
        ingested_at,
        row_number() over (partition by nct_id order by ingested_at desc) as load_rank
    from {{ source('bronze', 'ct_studies') }}
)

select protocol, ingested_at
from ranked
where load_rank = 1
