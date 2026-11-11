{#- Generic test: the combination of the given columns must be unique.
    Usage in YAML (model level):
      data_tests:
        - unique_combination:
            arguments:
              columns: [nct_id, condition_key] -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
