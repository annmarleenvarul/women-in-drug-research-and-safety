-- One row per drug ingredient seen in FAERS or in a trial. SCD Type 1: rebuilt every run,
-- so corrected names / classes simply overwrite the old values.
-- Natural key = normalised ingredient name (ClinicalTrials.gov has no RxCUI, so the name is
-- the only attribute both sources share).
with faers as (
    select
        ingredient_name,
        mode() within group (order by rxcui)      as rxcui,
        mode() within group (order by drug_class) as drug_class
    from {{ ref('stg_faers__suspect_drugs') }}
    group by ingredient_name
),

trials as (
    select distinct ingredient_name
    from {{ ref('stg_ct__study_drugs') }}
),

all_drugs as (
    select ingredient_name from faers
    union
    select ingredient_name from trials
)

select
    {{ surrogate_key(['a.ingredient_name']) }} as drug_key,
    f.rxcui,
    a.ingredient_name,
    f.drug_class,
    f.ingredient_name is not null              as appears_in_faers,
    t.ingredient_name is not null              as appears_in_trials
from all_drugs as a
left join faers  as f on f.ingredient_name = a.ingredient_name
left join trials as t on t.ingredient_name = a.ingredient_name
