-- S5: What keeping whole documents costs and buys.
-- Part A (cost): size of each layer's tables. Part B (benefit): top-level keys stored in bronze
-- that no model reads today; they stay available for future questions without re-calling the API.
with layer_sizes as (
    select
        'A. storage'                                                   as part,
        n.nspname || '.' || c.relname                                  as item,
        c.reltuples::bigint                                            as rows_estimate,
        pg_size_pretty(pg_total_relation_size(c.oid))                  as total_size,
        pg_total_relation_size(c.oid)                                  as sort_bytes
    from pg_class as c
    join pg_namespace as n on n.oid = c.relnamespace
    where n.nspname in ('{{ source("raw", "faers_reports").schema }}', 'silver', 'gold')
      and c.relkind = 'r'
),

used_faers as (
    select unnest(array['safetyreportid', 'safetyreportversion', 'receivedate', 'serious',
                        'seriousnessdeath', 'seriousnesshospitalization',
                        'seriousnesslifethreatening', 'patient']) as key
),

used_ctgov as (
    select unnest(array['identificationModule', 'statusModule', 'designModule',
                        'eligibilityModule', 'conditionsModule', 'armsInterventionsModule']) as key
),

faers_keys as (
    select k as key, count(*) as documents
    from {{ source('raw', 'faers_reports') }}, jsonb_object_keys(payload) as k
    group by k
),

ctgov_keys as (
    select k as key, count(*) as documents
    from {{ source('raw', 'ctgov_studies') }}, jsonb_object_keys(payload -> 'protocolSection') as k
    group by k
),

unused_keys as (
    select 'B. unused keys' as part, 'faers: ' || key as item, documents as rows_estimate,
           null as total_size, documents as sort_bytes
    from faers_keys where key not in (select key from used_faers)
    union all
    select 'B. unused keys', 'ctgov protocolSection: ' || key, documents, null, documents
    from ctgov_keys where key not in (select key from used_ctgov)
),

payload_size as (
    select 'A. storage' as part, 'avg payload, raw.faers_reports (bytes, compressed)' as item,
           avg(pg_column_size(payload))::bigint as rows_estimate, null as total_size, -1 as sort_bytes
    from {{ source('raw', 'faers_reports') }}
    union all
    select 'A. storage', 'avg payload, raw.ctgov_studies (bytes, compressed)',
           avg(pg_column_size(payload))::bigint, null, -1
    from {{ source('raw', 'ctgov_studies') }}
)

select part, item, rows_estimate as rows_or_value, total_size
from (
    select * from layer_sizes
    union all select * from payload_size
    union all select * from unused_keys
) as all_rows
order by part, sort_bytes desc, item
