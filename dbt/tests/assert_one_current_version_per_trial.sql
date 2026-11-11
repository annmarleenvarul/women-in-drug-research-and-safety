-- SCD Type 2 integrity: every trial must have exactly one current version in dim_trial.
select nct_id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_trial') }}
group by nct_id
having count(*) filter (where is_current) <> 1
