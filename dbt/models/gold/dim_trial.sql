-- One row per trial VERSION (SCD Type 2), built from the dbt snapshot.
select
    dbt_scd_id                                      as trial_key,
    nct_id,
    title,
    overall_status,
    phase,
    eligible_sex,
    min_age_years,
    max_age_years,
    dbt_valid_from::date                            as valid_from,
    coalesce(dbt_valid_to::date, date '9999-12-31') as valid_to,
    dbt_valid_to is null                            as is_current
from {{ ref('snap_ct__studies') }}
