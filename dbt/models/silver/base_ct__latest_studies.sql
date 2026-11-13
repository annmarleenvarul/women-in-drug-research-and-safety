{{ config(materialized='ephemeral') }}

-- Keep only the latest source version of each study.
-- Earlier states of a study are preserved by the snapshot (SCD Type 2), not here.
with ranked as (
    select
        payload -> 'protocolSection' as protocol,
        loaded_at as ingested_at,
        row_number() over (partition by nct_id order by source_updated_date desc, loaded_at desc) as load_rank
    from {{ source('raw', 'ctgov_studies') }}
)

select protocol, ingested_at
from ranked
where load_rank = 1
