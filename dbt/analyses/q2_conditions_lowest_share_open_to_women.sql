-- Q2: Which conditions have the lowest share of trials open to women?
-- Only conditions with at least var('min_trials_per_condition') trials, to avoid 1-of-1 = 0 %.
select
    c.condition_name,
    count(distinct t.nct_id)                                                   as trials,
    round(avg((t.eligible_sex in ('ALL', 'FEMALE'))::int) * 100, 1)            as pct_open_to_women
from {{ ref('bridge_trial_condition') }} as b
join {{ ref('dim_trial') }}              as t on t.nct_id = b.nct_id and t.is_current
join {{ ref('dim_condition') }}          as c on c.condition_key = b.condition_key
group by c.condition_name
having count(distinct t.nct_id) >= {{ var('min_trials_per_condition') }}
order by pct_open_to_women, trials desc
limit 20
