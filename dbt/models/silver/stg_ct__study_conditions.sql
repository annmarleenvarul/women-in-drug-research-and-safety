-- One row per condition per trial.
with conditions as (
    select
        s.protocol -> 'identificationModule' ->> 'nctId'           as nct_id,
        regexp_replace(trim(c.condition), '\s+', ' ', 'g')         as condition_name
    from {{ ref('base_ct__latest_studies') }} as s
    cross join lateral jsonb_array_elements_text(
        coalesce(s.protocol -> 'conditionsModule' -> 'conditions', '[]'::jsonb)
    ) as c(condition)
)

select distinct on (nct_id, lower(condition_name))
    nct_id,
    condition_name,
    -- natural key for dim_condition: 'Breast Cancer' and 'breast cancer' are one condition
    lower(condition_name) as condition_name_normalized
from conditions
where condition_name <> ''
order by nct_id, lower(condition_name), condition_name
