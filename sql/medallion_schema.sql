CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE bronze.fda_reports_raw (
    safetyreportid VARCHAR,
    receivedate VARCHAR,
    serious INTEGER,
    seriousnessdeath INTEGER,
    seriousnesshospitalization INTEGER,
    seriousnesslifethreatening INTEGER,
    patientonsetage DOUBLE,
    patientonsetageunit INTEGER,
    patientsex INTEGER,
    reactionmeddrapt VARCHAR,
    medicinalproduct VARCHAR,
    drugcharacterization VARCHAR
);

CREATE OR REPLACE TABLE bronze.ctg_studies_raw (
    nct_id VARCHAR,
    study_title VARCHAR,
    study_status VARCHAR,
    conditions VARCHAR,
    interventions VARCHAR,
    sex VARCHAR,
    age VARCHAR,
    phases VARCHAR,
    enrollment INTEGER,
    funder_type VARCHAR,
    study_type VARCHAR,
    start_date VARCHAR,
    completion_date VARCHAR,
    locations VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT
);

CREATE OR REPLACE TABLE gold.dim_drug (
    drug_key INTEGER PRIMARY KEY,
    rxcui VARCHAR,
    ingredient_name VARCHAR,
    drug_class VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_reaction (
    reaction_key INTEGER PRIMARY KEY,
    meddra_pt VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_patient_profile (
    patient_profile_key INTEGER PRIMARY KEY,
    sex VARCHAR,
    age_band VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_trial (
    trial_key INTEGER PRIMARY KEY,
    nct_id VARCHAR,
    title VARCHAR,
    overall_status VARCHAR,
    phase VARCHAR,
    eligible_sex VARCHAR,
    min_age_years NUMERIC(5,1),
    max_age_years NUMERIC(5,1),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE gold.dim_condition (
    condition_key INTEGER PRIMARY KEY,
    condition_name VARCHAR,
    condition_class VARCHAR
);

CREATE OR REPLACE TABLE gold.bridge_trial_condition (
    nct_id VARCHAR,
    condition_key INTEGER,
    PRIMARY KEY (nct_id, condition_key)
);

CREATE OR REPLACE TABLE gold.fact_trial_drug (
    trial_drug_key BIGINT PRIMARY KEY,
    trial_key INTEGER,
    drug_key INTEGER,
    start_date_key INTEGER,
    enrollment INTEGER,
    is_enrollment_actual BOOLEAN
);

CREATE OR REPLACE TABLE gold.fact_adverse_event (
    ae_key BIGINT PRIMARY KEY,
    report_id VARCHAR,
    received_date_key INTEGER,
    drug_key INTEGER,
    reaction_key INTEGER,
    patient_profile_key INTEGER,
    is_serious BOOLEAN,
    is_death BOOLEAN,
    is_hospitalisation BOOLEAN,
    is_life_threatening BOOLEAN
);

CREATE OR REPLACE VIEW silver.v_fda_clean AS
SELECT
    safetyreportid,
    CAST(receivedate AS DATE) AS received_date,
    CAST(serious AS BOOLEAN) AS is_serious,
    CASE
        WHEN patientsex = 1 THEN 'female'
        WHEN patientsex = 2 THEN 'male'
        ELSE 'unknown'
    END AS sex,
    CASE
        WHEN patientonsetageunit = 801 THEN '18-64'
        WHEN patientonsetageunit = 802 THEN '0-17'
        WHEN patientonsetageunit = 803 THEN '65+'
        ELSE 'unknown'
    END AS age_band,
    reactionmeddrapt,
    medicinalproduct
FROM bronze.fda_reports_raw;
