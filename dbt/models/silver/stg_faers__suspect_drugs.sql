-- One row per suspect drug per FAERS report.
-- drugcharacterization: 1 = suspect, 2 = concomitant, 3 = interacting. Only suspects are kept,
-- matching the fact grain from Project 1.
with drugs as (
    select
        r.payload ->> 'safetyreportid' as report_id,
        drug
    from {{ ref('base_faers__latest_reports') }} as r
    cross join lateral jsonb_array_elements(
        coalesce(r.payload -> 'patient' -> 'drug', '[]'::jsonb)
    ) as drug
    where drug ->> 'drugcharacterization' = '1'
),

named as (
    select
        report_id,
        -- prefer the openFDA-harmonised generic name, then active substance, then brand/free text
        {{ normalize_drug_name("coalesce(
            drug -> 'openfda' -> 'generic_name' ->> 0,
            drug -> 'activesubstance' ->> 'activesubstancename',
            drug ->> 'medicinalproduct')") }}           as ingredient_name,
        drug -> 'openfda' -> 'rxcui' ->> 0               as rxcui,
        drug -> 'openfda' -> 'pharm_class_epc' ->> 0     as drug_class
    from drugs
)

-- the same drug can be listed several times in one report (e.g. two products)
select distinct on (report_id, ingredient_name)
    report_id,
    ingredient_name,
    rxcui,
    drug_class
from named
where ingredient_name is not null
order by report_id, ingredient_name, rxcui nulls last
