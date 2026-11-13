-- Source tests run before any Silver table is replaced.
select report_id, report_version
from {{ source('raw', 'faers_reports') }}
where report_id = '' or report_version = '' or batch_id = ''
   or jsonb_typeof(payload) is distinct from 'object'
   or (payload ->> 'safetyreportid') is distinct from report_id
   or (payload ? 'safetyreportversion'
       and nullif(payload ->> 'safetyreportversion', '') is not null
       and payload ->> 'safetyreportversion' is distinct from report_version)
