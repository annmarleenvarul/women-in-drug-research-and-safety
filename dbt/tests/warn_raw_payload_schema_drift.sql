{{ config(severity='warn') }}

-- Schema-drift guard for the semi-structured bronze layer.
-- Bronze stores the API JSON as-is (schema-on-read), so if the API renames or drops a field
-- the load still succeeds and silver just gets NULLs. This test catches that: it returns any
-- REQUIRED path (macros/semi_structured_paths.sql) that no stored document contains.
-- Severity is warn: it alerts in the Airflow log (test_raw) without blocking the load,
-- because optional/absent fields are normal in this data.
with coverage as (
    {{ semi_structured_path_coverage() }}
)

select source, json_path, feeds, documents, documents_with_path
from coverage
where required
  and documents > 0
  and documents_with_path = 0
