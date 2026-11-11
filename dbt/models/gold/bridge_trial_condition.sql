-- Many-to-many link between trials and conditions (one trial can study several conditions).
-- Joined to the CURRENT row of dim_trial via nct_id.
select distinct
    nct_id,
    {{ surrogate_key(['condition_name_normalized']) }} as condition_key
from {{ ref('stg_ct__study_conditions') }}
