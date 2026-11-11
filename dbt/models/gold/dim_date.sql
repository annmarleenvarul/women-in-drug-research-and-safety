-- One row per calendar day. The range covers the configured bounds and is widened
-- automatically to include every report-received date and trial start date in the data.
with bounds as (
    select
        least(cast('{{ var("date_spine_start") }}' as date),
              (select min(received_date) from {{ ref('stg_faers__reports') }}),
              (select min(start_date)    from {{ ref('stg_ct__studies') }}))     as start_date,
        greatest(cast('{{ var("date_spine_end") }}' as date),
              (select max(received_date) from {{ ref('stg_faers__reports') }}),
              (select max(start_date)    from {{ ref('stg_ct__studies') }}))     as end_date
),

spine as (
    select generate_series(start_date, end_date, interval '1 day')::date as full_date
    from bounds
)

select
    {{ date_key('full_date') }}             as date_key,
    full_date,
    extract(year    from full_date)::smallint as year,
    extract(quarter from full_date)::smallint as quarter,
    extract(month   from full_date)::smallint as month,
    extract(day     from full_date)::smallint as day
from spine
