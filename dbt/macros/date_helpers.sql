{#- ClinicalTrials.gov dates may be 'YYYY-MM-DD' or only 'YYYY-MM' (then day 1). -#}
{% macro ct_partial_date(expr) -%}
    (case
        when ({{ expr }}) ~ '^\d{4}-\d{2}-\d{2}$' then ({{ expr }})::date
        when ({{ expr }}) ~ '^\d{4}-\d{2}$'       then (({{ expr }}) || '-01')::date
     end)
{%- endmacro %}

{#- DATE -> integer YYYYMMDD key used by dim_date. -#}
{% macro date_key(expr) -%}
    (to_char(({{ expr }}), 'YYYYMMDD')::int)
{%- endmacro %}
