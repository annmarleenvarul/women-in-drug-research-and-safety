-- Q5 / KPI 3: Do women report serious outcomes more often than men, by age group?
-- Seriousness is a report-level attribute, so the fact is first collapsed to one row per report.
with reports as (
    select
        f.report_id,
        p.sex,
        p.age_band,
        p.age_band_order,
        bool_or(f.is_serious)          as is_serious,
        bool_or(f.is_death)            as is_death,
        bool_or(f.is_hospitalisation)  as is_hospitalisation
    from {{ ref('fact_adverse_event') }}  as f
    join {{ ref('dim_patient_profile') }} as p on p.patient_profile_key = f.patient_profile_key
    where p.sex in ('Female', 'Male')
    group by f.report_id, p.sex, p.age_band, p.age_band_order
)

select
    age_band,
    sex,
    count(*)                                              as reports,
    round(avg(is_serious::int) * 100, 1)                  as pct_serious,
    round(avg(is_death::int) * 100, 1)                    as pct_death,
    round(avg(is_hospitalisation::int) * 100, 1)          as pct_hospitalisation
from reports
group by age_band, age_band_order, sex
order by age_band_order, sex
