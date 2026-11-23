-- S1: How often is each JSON path the pipeline reads actually present in the raw documents?
-- Sparse paths are why silver uses coalesce / NULL-tolerant extraction instead of NOT NULL columns.
with coverage as (
    {{ semi_structured_path_coverage() }}
)

select
    source,
    json_path,
    feeds,
    required,
    documents,
    documents_with_path,
    round(100.0 * documents_with_path / nullif(documents, 0), 1) as pct_present
from coverage
order by source, pct_present, json_path
