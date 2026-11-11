-- Q3 / KPI 2: Which drugs have the highest female share of adverse event reports?
-- The fact grain is drug x reaction x report, so reports are counted DISTINCT.
-- Female share = female / (female + male); reports with unknown sex are excluded.
-- in_trials shows whether the drug could be matched to a trial (links Q3 to KPI 1).
select
    dr.ingredient_name,
    dr.drug_class,
    dr.appears_in_trials                                                       as in_trials,
    count(distinct f.report_id) filter (where p.sex = 'Female')                as female_reports,
    count(distinct f.report_id) filter (where p.sex = 'Male')                  as male_reports,
    round(100.0 * count(distinct f.report_id) filter (where p.sex = 'Female')
          / nullif(count(distinct f.report_id) filter (where p.sex in ('Female', 'Male')), 0), 1)
                                                                               as pct_female
from {{ ref('fact_adverse_event') }}      as f
join {{ ref('dim_drug') }}                as dr on dr.drug_key = f.drug_key
join {{ ref('dim_patient_profile') }}     as p  on p.patient_profile_key = f.patient_profile_key
group by dr.ingredient_name, dr.drug_class, dr.appears_in_trials
having count(distinct f.report_id) filter (where p.sex in ('Female', 'Male'))
       >= {{ var('min_reports_per_drug') }}
order by pct_female desc, female_reports desc
limit 20
