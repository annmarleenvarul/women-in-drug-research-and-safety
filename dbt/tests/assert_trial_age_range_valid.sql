{{ config(severity='warn') }}
-- Data-quality warning (does not stop the pipeline): minimum eligible age above maximum age.
select nct_id, min_age_years, max_age_years
from {{ ref('stg_ct__studies') }}
where min_age_years > max_age_years
