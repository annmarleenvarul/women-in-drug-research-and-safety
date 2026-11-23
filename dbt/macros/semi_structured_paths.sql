{#- The JSON paths inside the raw (bronze) payloads that the silver models read.
    One list, used by:
      - analyses/semi_structured/s1_field_coverage.sql  (how often each path is present)
      - tests/warn_raw_payload_schema_drift.sql         (alerts if a path disappears)
    `required: true` = the pipeline loses a column if the path vanishes from the API;
    optional paths (e.g. seriousness flags) are legitimately absent on many records.
    Paths are SQL/JSON path expressions (PostgreSQL jsonpath, lax mode). -#}
{% macro semi_structured_paths() %}
    {{ return([
        {'source': 'faers', 'path': '$.safetyreportid',                               'feeds': 'report_id',              'required': true},
        {'source': 'faers', 'path': '$.receivedate',                                  'feeds': 'received_date_key',      'required': true},
        {'source': 'faers', 'path': '$.serious',                                      'feeds': 'is_serious',             'required': true},
        {'source': 'faers', 'path': '$.seriousnessdeath',                             'feeds': 'is_death',               'required': false},
        {'source': 'faers', 'path': '$.seriousnesshospitalization',                   'feeds': 'is_hospitalisation',     'required': false},
        {'source': 'faers', 'path': '$.seriousnesslifethreatening',                   'feeds': 'is_life_threatening',    'required': false},
        {'source': 'faers', 'path': '$.patient.patientsex',                           'feeds': 'dim_patient_profile.sex', 'required': true},
        {'source': 'faers', 'path': '$.patient.patientonsetage',                      'feeds': 'dim_patient_profile.age_band', 'required': true},
        {'source': 'faers', 'path': '$.patient.patientonsetageunit',                  'feeds': 'dim_patient_profile.age_band', 'required': true},
        {'source': 'faers', 'path': '$.patient.drug[*].drugcharacterization',         'feeds': 'suspect-drug filter',    'required': true},
        {'source': 'faers', 'path': '$.patient.drug[*].openfda.generic_name',         'feeds': 'dim_drug.ingredient_name (1st choice)', 'required': true},
        {'source': 'faers', 'path': '$.patient.drug[*].activesubstance.activesubstancename', 'feeds': 'dim_drug.ingredient_name (2nd choice)', 'required': false},
        {'source': 'faers', 'path': '$.patient.drug[*].medicinalproduct',             'feeds': 'dim_drug.ingredient_name (3rd choice)', 'required': true},
        {'source': 'faers', 'path': '$.patient.drug[*].openfda.pharm_class_epc',      'feeds': 'dim_drug.drug_class',    'required': false},
        {'source': 'faers', 'path': '$.patient.reaction[*].reactionmeddrapt',         'feeds': 'dim_reaction.meddra_pt', 'required': true},

        {'source': 'ctgov', 'path': '$.protocolSection.identificationModule.nctId',              'feeds': 'dim_trial.nct_id',        'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.identificationModule.briefTitle',         'feeds': 'dim_trial.title',         'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.statusModule.overallStatus',              'feeds': 'dim_trial.overall_status', 'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.statusModule.startDateStruct.date',       'feeds': 'fact_trial_drug.start_date_key', 'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.statusModule.lastUpdatePostDateStruct.date', 'feeds': 'snapshot version',  'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.designModule.phases',                     'feeds': 'dim_trial.phase',         'required': false},
        {'source': 'ctgov', 'path': '$.protocolSection.designModule.enrollmentInfo.count',       'feeds': 'fact_trial_drug.enrollment', 'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.designModule.enrollmentInfo.type',        'feeds': 'fact_trial_drug.is_enrollment_actual', 'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.eligibilityModule.sex',                   'feeds': 'dim_trial.eligible_sex',  'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.eligibilityModule.minimumAge',            'feeds': 'dim_trial.min_age_years', 'required': false},
        {'source': 'ctgov', 'path': '$.protocolSection.eligibilityModule.maximumAge',            'feeds': 'dim_trial.max_age_years', 'required': false},
        {'source': 'ctgov', 'path': '$.protocolSection.conditionsModule.conditions',             'feeds': 'dim_condition, bridge',   'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.armsInterventionsModule.interventions[*].type', 'feeds': 'drug/biological filter', 'required': true},
        {'source': 'ctgov', 'path': '$.protocolSection.armsInterventionsModule.interventions[*].name', 'feeds': 'dim_drug.ingredient_name', 'required': true}
    ]) }}
{% endmacro %}


{#- Presence count of every path above, one row per path. Shared by the
    coverage analysis and the drift test so both always check the same list. -#}
{% macro semi_structured_path_coverage() %}
    with docs as (
        select 'faers' as source, payload from {{ source('raw', 'faers_reports') }}
        union all
        select 'ctgov' as source, payload from {{ source('raw', 'ctgov_studies') }}
    )
    {% for p in semi_structured_paths() %}
    select
        '{{ p.source }}'   as source,
        '{{ p.path }}'     as json_path,
        '{{ p.feeds }}'    as feeds,
        {{ p.required }}   as required,
        count(*)           as documents,
        count(*) filter (where jsonb_path_exists(payload, '{{ p.path }}'::jsonpath)) as documents_with_path
    from docs
    where source = '{{ p.source }}'
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endmacro %}
