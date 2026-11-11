-- Q4: Which reactions are reported most often in women compared to men?
-- For each reaction: share of female reports that mention it vs share of male reports,
-- so the different total number of female and male reports does not bias the comparison.
with reports_by_sex as (
    select p.sex, count(distinct f.report_id) as n_reports
    from {{ ref('fact_adverse_event') }}  as f
    join {{ ref('dim_patient_profile') }} as p on p.patient_profile_key = f.patient_profile_key
    where p.sex in ('Female', 'Male')
    group by p.sex
),

reaction_reports as (
    select r.meddra_pt, p.sex, count(distinct f.report_id) as n_reports
    from {{ ref('fact_adverse_event') }}  as f
    join {{ ref('dim_reaction') }}        as r on r.reaction_key = f.reaction_key
    join {{ ref('dim_patient_profile') }} as p on p.patient_profile_key = f.patient_profile_key
    where p.sex in ('Female', 'Male')
    group by r.meddra_pt, p.sex
),

rates as (
    select
        rr.meddra_pt,
        sum(rr.n_reports) filter (where rr.sex = 'Female')                     as female_reports,
        sum(rr.n_reports) filter (where rr.sex = 'Male')                       as male_reports,
        max(100.0 * rr.n_reports / rs.n_reports) filter (where rr.sex = 'Female') as pct_of_female_reports,
        max(100.0 * rr.n_reports / rs.n_reports) filter (where rr.sex = 'Male')   as pct_of_male_reports
    from reaction_reports as rr
    join reports_by_sex   as rs on rs.sex = rr.sex
    group by rr.meddra_pt
)

select
    meddra_pt,
    coalesce(female_reports, 0)                       as female_reports,
    coalesce(male_reports, 0)                         as male_reports,
    round(coalesce(pct_of_female_reports, 0), 2)      as pct_of_female_reports,
    round(coalesce(pct_of_male_reports, 0), 2)        as pct_of_male_reports,
    round(coalesce(pct_of_female_reports, 0) - coalesce(pct_of_male_reports, 0), 2) as difference_pp
from rates
order by difference_pp desc
limit 20
