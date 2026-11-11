{{
    config(
        materialized='incremental',
        unique_key='report_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- Grain: one suspect drug x one reported reaction within one FAERS adverse event report.
--
-- Incremental load: only reports ingested since the last run are processed. The unique_key is
-- report_id (not ae_key) on purpose: delete+insert first removes ALL existing rows of a re-sent
-- report and then inserts its latest version, so a follow-up version that drops a drug or
-- reaction does not leave stale rows behind. Re-running the same load is therefore idempotent.

with reports as (
    select *
    from {{ ref('stg_faers__reports') }}
    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
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
