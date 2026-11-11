-- One row per MedDRA preferred term reported in FAERS (static dimension).
select distinct
    {{ surrogate_key(['meddra_pt']) }} as reaction_key,
    meddra_pt
from {{ ref('stg_faers__reactions') }}
