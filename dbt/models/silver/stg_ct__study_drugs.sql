-- One row per drug tested in a trial (interventions of type DRUG or BIOLOGICAL, placebos removed).
with interventions as (
    select
        s.protocol -> 'identificationModule' ->> 'nctId' as nct_id,
        i ->> 'type'                                    as intervention_type,
        {{ normalize_drug_name("i ->> 'name'") }}       as ingredient_name
    from {{ ref('base_ct__latest_studies') }} as s
    cross join lateral jsonb_array_elements(
        coalesce(s.protocol -> 'armsInterventionsModule' -> 'interventions', '[]'::jsonb)
    ) as i
)

select distinct
    nct_id,
    ingredient_name
from interventions
where intervention_type in ('DRUG', 'BIOLOGICAL')
  and ingredient_name is not null
  and ingredient_name !~ 'placebo'
