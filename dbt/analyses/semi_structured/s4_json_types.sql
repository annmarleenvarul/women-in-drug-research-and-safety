-- S4: JSON value types. The APIs send numbers and codes as strings, ages as "18 Years" text and
-- dates in two formats; silver casts and decodes them. Shows the JSON type and an example per path.
with typed as (
    select 'faers' as source, 'receivedate' as json_path,
           jsonb_typeof(payload -> 'receivedate') as json_type, payload ->> 'receivedate' as example
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'faers', 'serious', jsonb_typeof(payload -> 'serious'), payload ->> 'serious'
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'faers', 'patient.patientsex', jsonb_typeof(payload #> '{patient,patientsex}'), payload #>> '{patient,patientsex}'
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'faers', 'patient.patientonsetage', jsonb_typeof(payload #> '{patient,patientonsetage}'), payload #>> '{patient,patientonsetage}'
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'faers', 'patient.patientonsetageunit', jsonb_typeof(payload #> '{patient,patientonsetageunit}'), payload #>> '{patient,patientonsetageunit}'
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'faers', 'patient.drug[*].openfda.generic_name', jsonb_typeof(d -> 'openfda' -> 'generic_name'), (d -> 'openfda' -> 'generic_name')::text
    from {{ source('raw', 'faers_reports') }}
    cross join lateral jsonb_array_elements(coalesce(payload #> '{patient,drug}', '[]')) as d
    union all
    select 'ctgov', 'designModule.phases', jsonb_typeof(payload #> '{protocolSection,designModule,phases}'),
           (payload #> '{protocolSection,designModule,phases}')::text
    from {{ source('raw', 'ctgov_studies') }}
    union all
    select 'ctgov', 'designModule.enrollmentInfo.count', jsonb_typeof(payload #> '{protocolSection,designModule,enrollmentInfo,count}'),
           payload #>> '{protocolSection,designModule,enrollmentInfo,count}'
    from {{ source('raw', 'ctgov_studies') }}
    union all
    select 'ctgov', 'eligibilityModule.minimumAge', jsonb_typeof(payload #> '{protocolSection,eligibilityModule,minimumAge}'),
           payload #>> '{protocolSection,eligibilityModule,minimumAge}'
    from {{ source('raw', 'ctgov_studies') }}
    union all
    -- partial dates: "2024-03" (month precision) vs "2024-03-15" (day precision)
    select 'ctgov', 'statusModule.startDateStruct.date',
           jsonb_typeof(payload #> '{protocolSection,statusModule,startDateStruct,date}')
               || case length(payload #>> '{protocolSection,statusModule,startDateStruct,date}')
                      when 7 then ' (YYYY-MM)' when 10 then ' (YYYY-MM-DD)' else '' end,
           payload #>> '{protocolSection,statusModule,startDateStruct,date}'
    from {{ source('raw', 'ctgov_studies') }}
)

select
    source,
    json_path,
    coalesce(json_type, 'missing')   as json_type,
    count(*)                         as occurrences,
    min(example)                     as example_value
from typed
group by source, json_path, coalesce(json_type, 'missing')
order by source, json_path, occurrences desc
