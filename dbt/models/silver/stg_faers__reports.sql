-- One row per FAERS safety report (latest version): decoded patient and seriousness fields.
with reports as (
    select * from {{ ref('base_faers__latest_reports') }}
),

decoded as (
    select
        payload ->> 'safetyreportid'                              as report_id,
        to_date(payload ->> 'receivedate', 'YYYYMMDD')            as received_date,
        case payload -> 'patient' ->> 'patientsex'
            when '1' then 'Male'
            when '2' then 'Female'
            else 'Unknown'
        end                                                        as sex,
        {{ faers_age_in_years("payload -> 'patient' ->> 'patientonsetage'",
                              "payload -> 'patient' ->> 'patientonsetageunit'") }} as raw_age_years,
        -- openFDA encodes flags as '1' = yes; anything else (missing, '2') = no
        coalesce(payload ->> 'serious', '') = '1'                  as is_serious,
        coalesce(payload ->> 'seriousnessdeath', '') = '1'         as is_death,
        coalesce(payload ->> 'seriousnesshospitalization', '') = '1' as is_hospitalisation,
        coalesce(payload ->> 'seriousnesslifethreatening', '') = '1' as is_life_threatening,
        ingested_at
    from reports
)

select
    report_id,
    received_date,
    sex,
    -- implausible ages are treated as unknown
    case when raw_age_years between 0 and 120 then raw_age_years end as age_years,
    {{ age_band("case when raw_age_years between 0 and 120 then raw_age_years end") }} as age_band,
    is_serious,
    is_death,
    is_hospitalisation,
    is_life_threatening,
    ingested_at
from decoded
