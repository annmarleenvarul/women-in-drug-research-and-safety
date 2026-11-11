-- Static dimension: every combination of sex and age band (3 x 5 = 15 rows).
-- Generated rather than derived from data, so every profile exists even before it is used.
with sexes as (
    select * from (values ('Female'), ('Male'), ('Unknown')) as s(sex)
),

age_bands as (
    select * from (values
        ('0-17', 1), ('18-44', 2), ('45-64', 3), ('65+', 4), ('Unknown', 5)
    ) as a(age_band, age_band_order)
)

select
    {{ surrogate_key(['s.sex', 'a.age_band']) }} as patient_profile_key,
    s.sex,
    a.age_band,
    a.age_band_order
from sexes as s
cross join age_bands as a
