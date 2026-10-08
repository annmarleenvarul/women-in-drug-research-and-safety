-- Dimesion tables

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT
);


CREATE TABLE dim_drug (
    drug_key SERIAL PRIMARY KEY,
    rxcui VARCHAR(12) UNIQUE,
    ingredient_name VARCHAR(100),
    drug_class VARCHAR(50)
);


CREATE TABLE dim_reaction (
    reaction_key SERIAL PRIMARY KEY,
    meddra_pt VARCHAR(100) UNIQUE
);


CREATE TABLE dim_patient_profile (
    patient_profile_key SERIAL PRIMARY KEY,
    sex VARCHAR(7),
    age_band VARCHAR(10)
);


CREATE TABLE dim_trial (
    trial_key SERIAL PRIMARY KEY,
    nct_id CHAR(11),
    title TEXT,
    overall_status VARCHAR(30),
    phase VARCHAR(20),
    eligible_sex VARCHAR(6),
    min_age_years NUMERIC(5,1),
    max_age_years NUMERIC(5,1),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);


CREATE TABLE dim_condition (
    condition_key SERIAL PRIMARY KEY,
    condition_name VARCHAR(300) UNIQUE,
    condition_class VARCHAR(200)
);


-- Briding table

CREATE TABLE bridge_trial_condition (
    nct_id CHAR(11),
    condition_key INT,
    PRIMARY KEY (nct_id, condition_key),
    FOREIGN KEY (condition_key) REFERENCES dim_condition(condition_key)
);


-- Fact tables

CREATE TABLE fact_trial_drug (
    trial_drug_key BIGSERIAL PRIMARY KEY,
    trial_key INT,
    drug_key INT,
    start_date_key INT,
    enrollment INT,
    is_enrollment_actual BOOLEAN,
    FOREIGN KEY (trial_key) REFERENCES dim_trial(trial_key),
    FOREIGN KEY (drug_key) REFERENCES dim_drug(drug_key),
    FOREIGN KEY (start_date_key) REFERENCES dim_date(date_key)
);


CREATE TABLE fact_adverse_event (
    ae_key BIGSERIAL PRIMARY KEY,
    report_id VARCHAR(20),
    received_date_key INT,
    drug_key INT,
    reaction_key INT,
    patient_profile_key INT,
    is_serious BOOLEAN,
    is_death BOOLEAN,
    is_hospitalisation BOOLEAN,
    is_life_threatening BOOLEAN,
    FOREIGN KEY (received_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (drug_key) REFERENCES dim_drug(drug_key),
    FOREIGN KEY (reaction_key) REFERENCES dim_reaction(reaction_key),
    FOREIGN KEY (patient_profile_key) REFERENCES dim_patient_profile(patient_profile_key)
);