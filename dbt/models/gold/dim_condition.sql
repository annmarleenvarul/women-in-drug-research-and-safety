-- One row per condition (SCD Type 1). Spelling variants that differ only in case are merged;
-- the most common spelling is used as the display name.
select
    {{ surrogate_key(['condition_name_normalized']) }}         as condition_key,
    mode() within group (order by condition_name)              as condition_name
from {{ ref('stg_ct__study_conditions') }}
group by condition_name_normalized
