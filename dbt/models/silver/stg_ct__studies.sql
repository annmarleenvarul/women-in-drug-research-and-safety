-- One row per clinical trial (latest ingested state), flattened from API v2 JSON.
with studies as (
    select * from {{ ref('base_ct__latest_studies') }}
)

select
    protocol -> 'identificationModule' ->> 'nctId'                     as nct_id,
    protocol -> 'identificationModule' ->> 'briefTitle'                as title,
    protocol -> 'statusModule' ->> 'overallStatus'                     as overall_status,
    -- phases is an array, e.g. ["PHASE2","PHASE3"] -> 'PHASE2/PHASE3'; missing -> 'NA'
    coalesce(
        (select string_agg(ph, '/' order by ph)
         from jsonb_array_elements_text(protocol -> 'designModule' -> 'phases') as ph),
        'NA')                                                          as phase,
    upper(protocol -> 'eligibilityModule' ->> 'sex')                   as eligible_sex,
    {{ ct_age_in_years("protocol -> 'eligibilityModule' ->> 'minimumAge'") }} as min_age_years,
    {{ ct_age_in_years("protocol -> 'eligibilityModule' ->> 'maximumAge'") }} as max_age_years,
    {{ ct_partial_date("protocol -> 'statusModule' -> 'startDateStruct' ->> 'date'") }} as start_date,
    (protocol -> 'designModule' -> 'enrollmentInfo' ->> 'count')::int  as enrollment,
    (protocol -> 'designModule' -> 'enrollmentInfo' ->> 'type') = 'ACTUAL' as is_enrollment_actual,
    -- used by the snapshot to detect a new version of the trial
    {{ ct_partial_date("protocol -> 'statusModule' -> 'lastUpdatePostDateStruct' ->> 'date'") }}::timestamp
                                                                       as last_updated_at,
    ingested_at
from studies
