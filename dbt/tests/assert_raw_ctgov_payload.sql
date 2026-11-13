select nct_id, source_updated_date
from {{ source('raw', 'ctgov_studies') }}
where nct_id = '' or batch_id = ''
   or jsonb_typeof(payload) is distinct from 'object'
   or (payload #>> '{protocolSection,identificationModule,nctId}') is distinct from nct_id
   or (payload #>> '{protocolSection,statusModule,lastUpdatePostDateStruct,date}')
       is distinct from to_char(source_updated_date, 'YYYY-MM-DD')
