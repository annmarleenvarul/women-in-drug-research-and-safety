-- Q1 / KPI 1: What share of trials are open to women, by start year and phase?
-- Counted per trial (current version), not per trial-drug row.
with trials as (
    select distinct t.nct_id, t.phase, t.eligible_sex, d.year as start_year
    from {{ ref('fact_trial_drug') }} as f
    join {{ ref('dim_trial') }}       as t on t.trial_key = f.trial_key
    left join {{ ref('dim_date') }}   as d on d.date_key  = f.start_date_key
)

select
    start_year,
    phase,
    count(*)                                                              as trials,
    round(avg((eligible_sex in ('ALL', 'FEMALE'))::int) * 100, 1)         as pct_open_to_women,
    round(avg((eligible_sex = 'FEMALE')::int) * 100, 1)                   as pct_women_only,
    round(avg((eligible_sex = 'MALE')::int) * 100, 1)                     as pct_men_only,
    round(avg((eligible_sex = 'ALL')::int) * 100, 1)                      as pct_all_sexes
from trials
group by start_year, phase
order by start_year, phase
