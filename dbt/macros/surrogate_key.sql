{#- Deterministic hash key built from natural-key columns.
    Hash keys (instead of SERIAL) stay identical across full rebuilds of the
    dimensions, so the incremental fact table never ends up with stale foreign keys. -#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for col in columns %}
        coalesce(cast({{ col }} as text), '_null_')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{%- endmacro %}
