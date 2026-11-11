{#- Normalise a free-text drug name so FAERS generic names and ClinicalTrials.gov
    intervention names can be matched, e.g.
      'METFORMIN HYDROCHLORIDE'           -> 'metformin'
      'Metformin 500 mg tablets'          -> 'metformin'
      'Pembrolizumab (MK-3475) injection' -> 'pembrolizumab'
    Steps: lowercase -> drop (...) -> drop doses -> punctuation to spaces ->
    collapse spaces -> strip trailing salt / dosage-form words. -#}
{% macro normalize_drug_name(expr) -%}
    nullif(trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower({{ expr }}), '\([^)]*\)', ' ', 'g'),
                        '\m\d+(\.\d+)?\s*(mg|mcg|ug|g|ml|iu|units?|%)(/(kg|m2|ml))?', ' ', 'g'),
                    '[^a-z0-9]+', ' ', 'g'),
                '\s+', ' ', 'g'),
            '(\s+(hydrochloride|hcl|hydrobromide|sodium|potassium|sulfate|sulphate|maleate|mesylate|tartrate|citrate|acetate|phosphate|besylate|succinate|fumarate|tablets?|capsules?|injection|injectable|infusion|oral|solution|suspension|iv|sc|cream|gel|ointment|er|xr|sr))+\s*$',
            '', 'g')
    ), '')
{%- endmacro %}
