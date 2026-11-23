-- S2: Nested arrays and fan-out. One JSON document becomes many rows in silver and gold;
-- this shows how many elements each array holds and how many fact rows one document produces.
-- Uses the latest version of each document (same base models as silver).
with faers as (
    select
        r.payload ->> 'safetyreportid'                                              as doc_id,
        jsonb_array_length(coalesce(r.payload -> 'patient' -> 'drug', '[]'))         as drugs_all,
        jsonb_array_length(jsonb_path_query_array(
            r.payload, '$.patient.drug[*] ? (@.drugcharacterization == "1")'))       as drugs_suspect,
        jsonb_array_length(coalesce(r.payload -> 'patient' -> 'reaction', '[]'))     as reactions
    from {{ ref('base_faers__latest_reports') }} as r
),

faers_fact as (
    select report_id as doc_id, count(*) as fact_rows
    from {{ ref('fact_adverse_event') }}
    group by report_id
),

ctgov as (
    select
        s.protocol -> 'identificationModule' ->> 'nctId'                                     as doc_id,
        jsonb_array_length(coalesce(s.protocol -> 'conditionsModule' -> 'conditions', '[]'))  as conditions,
        jsonb_array_length(coalesce(s.protocol -> 'armsInterventionsModule' -> 'interventions', '[]')) as interventions_all,
        jsonb_array_length(jsonb_path_query_array(
            s.protocol, '$.armsInterventionsModule.interventions[*] ? (@.type == "DRUG" || @.type == "BIOLOGICAL")'))
                                                                                             as interventions_drug,
        jsonb_array_length(coalesce(s.protocol -> 'designModule' -> 'phases', '[]'))          as phases
    from {{ ref('base_ct__latest_studies') }} as s
),

ctgov_fact as (
    select t.nct_id as doc_id, count(*) as fact_rows
    from {{ ref('fact_trial_drug') }} as f
    join {{ ref('dim_trial') }}       as t on t.trial_key = f.trial_key
    group by t.nct_id
),

faers_all as (
    select f.*, coalesce(x.fact_rows, 0) as fact_rows
    from faers as f left join faers_fact as x using (doc_id)
),

ctgov_all as (
    select c.*, coalesce(x.fact_rows, 0) as fact_rows
    from ctgov as c left join ctgov_fact as x using (doc_id)
)

{% set metrics = [
    ('faers', 'patient.drug[] (all drugs)',             'faers_all', 'drugs_all'),
    ('faers', 'patient.drug[] (suspect only)',          'faers_all', 'drugs_suspect'),
    ('faers', 'patient.reaction[]',                     'faers_all', 'reactions'),
    ('faers', '=> fact_adverse_event rows per report',  'faers_all', 'fact_rows'),
    ('ctgov', 'conditionsModule.conditions[]',          'ctgov_all', 'conditions'),
    ('ctgov', 'interventions[] (all types)',            'ctgov_all', 'interventions_all'),
    ('ctgov', 'interventions[] (DRUG/BIOLOGICAL)',      'ctgov_all', 'interventions_drug'),
    ('ctgov', 'designModule.phases[]',                  'ctgov_all', 'phases'),
    ('ctgov', '=> fact_trial_drug rows per trial',      'ctgov_all', 'fact_rows'),
] %}

{% for source, label, cte, col in metrics %}
select
    '{{ source }}'                                              as source,
    '{{ label }}'                                               as array_or_output,
    count(*)                                                    as documents,
    min({{ col }})                                              as min_per_doc,
    round(avg({{ col }}), 2)                                    as avg_per_doc,
    percentile_disc(0.5) within group (order by {{ col }})      as median_per_doc,
    max({{ col }})                                              as max_per_doc,
    sum({{ col }})                                              as total_elements,
    count(*) filter (where {{ col }} = 0)                       as docs_with_zero
from {{ cte }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
