-- Grain: one drug x one clinical trial.
-- trial_key points to the version of the trial that was current when the fact was loaded.
with trial_drugs as (
    select * from {{ ref('stg_ct__study_drugs') }}
),

studies as (
    select * from {{ ref('stg_ct__studies') }}
),

current_trials as (
    select trial_key, nct_id
    from {{ ref('dim_trial') }}
    where is_current
)

select
    {{ surrogate_key(['td.nct_id', 'td.ingredient_name']) }} as trial_drug_key,
    ct.trial_key,
    {{ surrogate_key(['td.ingredient_name']) }}               as drug_key,
    {{ date_key('s.start_date') }}                           as start_date_key,
    s.enrollment,
    s.is_enrollment_actual
from trial_drugs as td
join studies        as s  on s.nct_id  = td.nct_id
join current_trials as ct on ct.nct_id = td.nct_id
