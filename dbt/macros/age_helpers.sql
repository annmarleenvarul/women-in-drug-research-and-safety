{#- FAERS age: patientonsetage + patientonsetageunit code
    (800 decade, 801 year, 802 month, 803 week, 804 day, 805 hour).
    Returns NULL for non-numeric values, unknown units or implausible ages. -#}
{% macro faers_age_in_years(age_expr, unit_expr) -%}
    (case
        when ({{ age_expr }}) ~ '^[0-9]+(\.[0-9]+)?$' then
            round(({{ age_expr }})::numeric * case ({{ unit_expr }})
                when '800' then 10
                when '801' then 1
                when '802' then 1 / 12.0
                when '803' then 1 / 52.18
                when '804' then 1 / 365.25
                when '805' then 1 / 8766.0
            end, 1)
     end)
{%- endmacro %}

{#- ClinicalTrials.gov ages are strings such as '18 Years', '6 Months', '30 Days'. -#}
{% macro ct_age_in_years(expr) -%}
    (case
        when ({{ expr }}) ~* '^\s*[0-9.]+\s+(year|month|week|day|hour|minute)s?\s*$' then
            round(
                split_part(trim(({{ expr }})), ' ', 1)::numeric
                * case lower(regexp_replace(split_part(trim(({{ expr }})), ' ', 2), 's$', ''))
                    when 'year'   then 1
                    when 'month'  then 1 / 12.0
                    when 'week'   then 1 / 52.18
                    when 'day'    then 1 / 365.25
                    when 'hour'   then 1 / 8766.0
                    when 'minute' then 1 / 525960.0
                  end
            , 1)
     end)
{%- endmacro %}

{#- Age bands from Project 1's dim_patient_profile. -#}
{% macro age_band(age_years_expr) -%}
    (case
        when ({{ age_years_expr }}) is null then 'Unknown'
        when ({{ age_years_expr }}) < 18 then '0-17'
        when ({{ age_years_expr }}) < 45 then '18-44'
        when ({{ age_years_expr }}) < 65 then '45-64'
        else '65+'
     end)
{%- endmacro %}
