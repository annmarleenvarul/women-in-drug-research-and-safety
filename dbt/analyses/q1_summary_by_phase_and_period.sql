-- Q1 summary (README): share of trials open to women by phase and by start period.
-- Same logic as q1_trials_open_to_women_by_year_phase, rolled up so it fits on one screen.
with trials as (
    select distinct t.nct_id, t.phase, t.eligible_sex, d.year as start_year
    from {{ ref('fact_trial_drug') }} as f
    join {{ ref('dim_trial') }}       as t on t.trial_key = f.trial_key
    left join {{ ref('dim_date') }}   as d on d.date_key  = f.start_date_key
),

labelled as (
    select
        phase,
        case
            when start_year is null then 'unknown'
            when start_year < 2010  then 'before 2010'
            when start_year < 2020  then '2010-2019'
            else '2020 or later'
        end as start_period,
        eligible_sex
    from trials
),

grouped as (
    select
        case when grouping(phase) = 0 then 'phase' else 'start period' end as grouped_by,
        coalesce(phase, start_period)                                     as grp,
        count(*)                                                          as trials,
        round(avg((eligible_sex in ('ALL', 'FEMALE'))::int) * 100, 1)     as pct_open_to_women,
        round(avg((eligible_sex = 'FEMALE')::int) * 100, 1)               as pct_women_only,
        round(avg((eligible_sex = 'MALE')::int) * 100, 1)                 as pct_men_only,
        round(avg((eligible_sex = 'ALL')::int) * 100, 1)                  as pct_all_sexes
    from labelled
    group by grouping sets ((phase), (start_period))
)

select *
from grouped
order by grouped_by, grp
