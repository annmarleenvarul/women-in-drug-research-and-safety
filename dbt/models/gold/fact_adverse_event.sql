{{ config(materialized='table') }}

-- Grain: one suspect drug x reaction x latest FAERS report.
-- Rebuild from the validated Silver state, like the other Gold tables. This also
-- removes obsolete rows when a correction drops ALL suspect drugs or reactions.
with reports as (
    select *
    from {{ ref('stg_faers__reports') }}
),

drugs as (
    select report_id, ingredient_name from {{ ref('stg_faers__suspect_drugs') }}
),

reactions as (
    select report_id, meddra_pt from {{ ref('stg_faers__reactions') }}
)

select
    {{ surrogate_key(['r.report_id', 'd.ingredient_name', 'x.meddra_pt']) }} as ae_key,
    r.report_id,
    {{ date_key('r.received_date') }}                         as received_date_key,
    {{ surrogate_key(['d.ingredient_name']) }}                as drug_key,
    {{ surrogate_key(['x.meddra_pt']) }}                      as reaction_key,
    {{ surrogate_key(['r.sex', 'r.age_band']) }}              as patient_profile_key,
    r.is_serious,
    r.is_death,
    r.is_hospitalisation,
    r.is_life_threatening,
    r.ingested_at
from reports   as r
join drugs     as d on d.report_id = r.report_id
join reactions as x on x.report_id = r.report_id
