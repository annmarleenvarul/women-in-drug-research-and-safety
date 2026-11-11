-- SCD Type 2 integrity: versions of the same trial must not overlap in time.
select a.nct_id, a.trial_key, b.trial_key as overlapping_trial_key
from {{ ref('dim_trial') }} as a
join {{ ref('dim_trial') }} as b
  on a.nct_id = b.nct_id
 and a.trial_key <> b.trial_key
 and a.valid_from < b.valid_to
 and b.valid_from < a.valid_to
