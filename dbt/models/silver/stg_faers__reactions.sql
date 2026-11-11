-- One row per reported reaction (MedDRA preferred term) per FAERS report.
with reactions as (
    select
        r.payload ->> 'safetyreportid' as report_id,
        trim(reaction ->> 'reactionmeddrapt') as pt
    from {{ ref('base_faers__latest_reports') }} as r
    cross join lateral jsonb_array_elements(
        coalesce(r.payload -> 'patient' -> 'reaction', '[]'::jsonb)
    ) as reaction
)

select distinct
    report_id,
    -- standardise capitalisation: 'NAUSEA' / 'nausea' -> 'Nausea'
    upper(left(pt, 1)) || lower(substr(pt, 2)) as meddra_pt
from reactions
where pt is not null and pt <> ''
