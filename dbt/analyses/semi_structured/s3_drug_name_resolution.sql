-- S3: Heterogeneous nested structure. A FAERS suspect drug names its ingredient in one of
-- three places, depending on whether openFDA could harmonise the record. stg_faers__suspect_drugs
-- takes the first one present; this shows how often each fallback is needed.
with suspect_drugs as (
    select drug
    from {{ ref('base_faers__latest_reports') }} as r
    cross join lateral jsonb_array_elements(coalesce(r.payload -> 'patient' -> 'drug', '[]')) as drug
    where drug ->> 'drugcharacterization' = '1'
),

resolved as (
    select
        case
            when drug -> 'openfda' -> 'generic_name' ->> 0 is not null          then '1. openfda.generic_name[0]'
            when drug -> 'activesubstance' ->> 'activesubstancename' is not null then '2. activesubstance.activesubstancename'
            when drug ->> 'medicinalproduct' is not null                         then '3. medicinalproduct (free text)'
            else '4. no name: row dropped'
        end                                                          as name_taken_from,
        drug ? 'openfda'                                             as has_openfda_block,
        drug -> 'openfda' -> 'rxcui' ->> 0 is not null               as has_rxcui,
        drug -> 'openfda' -> 'pharm_class_epc' ->> 0 is not null     as has_drug_class
    from suspect_drugs
)

select
    name_taken_from,
    count(*)                                                          as suspect_drug_elements,
    round(100.0 * count(*) / sum(count(*)) over (), 1)                as pct,
    count(*) filter (where has_openfda_block)                         as with_openfda_block,
    count(*) filter (where has_rxcui)                                 as with_rxcui,
    count(*) filter (where has_drug_class)                            as with_drug_class
from resolved
group by name_taken_from
order by name_taken_from
