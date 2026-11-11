-- =====================================================================
-- Sample bronze data for developing/testing the dbt project before the
-- Airflow ingestion is ready. Tables follow the contract in
-- models/silver/_sources.yml. JSON mirrors the real API structures.
--   psql -d warehouse -f scripts/sample_bronze.sql
-- =====================================================================
create schema if not exists bronze;

create table if not exists bronze.faers_reports (
    safetyreportid text        not null,
    payload        jsonb       not null,
    load_date      date        not null,
    ingested_at    timestamptz not null default now()
);

create table if not exists bronze.ct_studies (
    nct_id      text        not null,
    payload     jsonb       not null,
    load_date   date        not null,
    ingested_at timestamptz not null default now()
);

truncate bronze.faers_reports, bronze.ct_studies;

-- ---------------- FAERS (openFDA /drug/event results) ----------------
insert into bronze.faers_reports (safetyreportid, payload, load_date, ingested_at) values
('10000001', $j${"safetyreportid":"10000001","safetyreportversion":"1","receivedate":"20260105","serious":"1","seriousnesshospitalization":"1",
  "patient":{"patientsex":"2","patientonsetage":"52","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"GLUCOPHAGE","openfda":{"generic_name":["METFORMIN HYDROCHLORIDE"],"rxcui":["861007"],"pharm_class_epc":["Biguanide [EPC]"]}},
           {"drugcharacterization":"2","medicinalproduct":"ASPIRIN","openfda":{"generic_name":["ASPIRIN"]}}],
   "reaction":[{"reactionmeddrapt":"Lactic acidosis"},{"reactionmeddrapt":"NAUSEA"}]}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

('10000002', $j${"safetyreportid":"10000002","safetyreportversion":"1","receivedate":"20260105","serious":"2",
  "patient":{"patientsex":"1","patientonsetage":"67","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"METFORMIN","openfda":{"generic_name":["METFORMIN HYDROCHLORIDE"],"rxcui":["861007"],"pharm_class_epc":["Biguanide [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Diarrhoea"}]}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

('10000003', $j${"safetyreportid":"10000003","safetyreportversion":"1","receivedate":"20260105","serious":"1","seriousnesslifethreatening":"1",
  "patient":{"patientsex":"2","patientonsetage":"4","patientonsetageunit":"800",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"KEYTRUDA","openfda":{"generic_name":["PEMBROLIZUMAB"],"rxcui":["1547545"],"pharm_class_epc":["Programmed Death Receptor-1 Blocking Antibody [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Pneumonitis"}]}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

-- newer version of the same report arrives later: must replace version 1
('10000003', $j${"safetyreportid":"10000003","safetyreportversion":"2","receivedate":"20260105","serious":"1","seriousnesslifethreatening":"1","seriousnesshospitalization":"1",
  "patient":{"patientsex":"2","patientonsetage":"4","patientonsetageunit":"800",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"KEYTRUDA","openfda":{"generic_name":["PEMBROLIZUMAB"],"rxcui":["1547545"],"pharm_class_epc":["Programmed Death Receptor-1 Blocking Antibody [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Pneumonitis"},{"reactionmeddrapt":"Colitis"}]}}$j$, '2026-01-06', '2026-01-07 02:00+00'),

('10000004', $j${"safetyreportid":"10000004","safetyreportversion":"1","receivedate":"20260106","serious":"1","seriousnessdeath":"1",
  "patient":{"patientsex":"1","patientonsetage":"58","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"PROPECIA","openfda":{"generic_name":["FINASTERIDE"],"rxcui":["310346"],"pharm_class_epc":["5-alpha Reductase Inhibitor [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Depression"}]}}$j$, '2026-01-06', '2026-01-07 02:00+00'),

('10000005', $j${"safetyreportid":"10000005","safetyreportversion":"1","receivedate":"20260106","serious":"2",
  "patient":{"patientsex":"2","patientonsetage":"8","patientonsetageunit":"802",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"HUMIRA","openfda":{"generic_name":["ADALIMUMAB"],"rxcui":["327361"],"pharm_class_epc":["Tumor Necrosis Factor Blocker [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Injection site reaction"}]}}$j$, '2026-01-06', '2026-01-07 02:00+00'),

-- unknown sex, implausible age (999 years) -> sex Unknown, age band Unknown
('10000006', $j${"safetyreportid":"10000006","safetyreportversion":"1","receivedate":"20260106","serious":"1","seriousnesshospitalization":"1",
  "patient":{"patientonsetage":"999","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"HUMIRA","activesubstance":{"activesubstancename":"ADALIMUMAB"}},
           {"drugcharacterization":"1","medicinalproduct":"METHOTREXATE SODIUM"}],
   "reaction":[{"reactionmeddrapt":"Pneumonia"},{"reactionmeddrapt":"pneumonia"}]}}$j$, '2026-01-06', '2026-01-07 02:00+00');

-- ---------------- ClinicalTrials.gov (API v2 /studies) ----------------
insert into bronze.ct_studies (nct_id, payload, load_date, ingested_at) values
('NCT00000001', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000001","briefTitle":"Metformin in Early Breast Cancer"},
  "statusModule":{"overallStatus":"RECRUITING","startDateStruct":{"date":"2022-04-15"},"lastUpdatePostDateStruct":{"date":"2025-11-02"}},
  "designModule":{"phases":["PHASE2"],"enrollmentInfo":{"count":120,"type":"ESTIMATED"}},
  "eligibilityModule":{"sex":"ALL","minimumAge":"18 Years","maximumAge":"75 Years"},
  "conditionsModule":{"conditions":["Breast Cancer"]},
  "armsInterventionsModule":{"interventions":[{"type":"DRUG","name":"Metformin 500 mg tablets"},{"type":"DRUG","name":"Placebo oral tablet"}]}}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

('NCT00000002', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000002","briefTitle":"Pembrolizumab in Triple-Negative Breast Cancer"},
  "statusModule":{"overallStatus":"COMPLETED","startDateStruct":{"date":"2019-06-01"},"lastUpdatePostDateStruct":{"date":"2024-03-10"}},
  "designModule":{"phases":["PHASE3"],"enrollmentInfo":{"count":1174,"type":"ACTUAL"}},
  "eligibilityModule":{"sex":"FEMALE","minimumAge":"18 Years"},
  "conditionsModule":{"conditions":["Breast Cancer","breast cancer ","Triple Negative Breast Neoplasms"]},
  "armsInterventionsModule":{"interventions":[{"type":"BIOLOGICAL","name":"Pembrolizumab (MK-3475) injection"},{"type":"DRUG","name":"Placebo"}]}}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

('NCT00000003', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000003","briefTitle":"Finasteride for Prostate Cancer Prevention"},
  "statusModule":{"overallStatus":"ACTIVE_NOT_RECRUITING","startDateStruct":{"date":"2020-01"},"lastUpdatePostDateStruct":{"date":"2025-08-20"}},
  "designModule":{"phases":["PHASE3","PHASE2"],"enrollmentInfo":{"count":800,"type":"ACTUAL"}},
  "eligibilityModule":{"sex":"MALE","minimumAge":"50 Years","maximumAge":"80 Years"},
  "conditionsModule":{"conditions":["Prostate Cancer"]},
  "armsInterventionsModule":{"interventions":[{"type":"DRUG","name":"Finasteride 5 mg"}]}}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

('NCT00000004', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000004","briefTitle":"Adalimumab in Juvenile Idiopathic Arthritis"},
  "statusModule":{"overallStatus":"RECRUITING","startDateStruct":{"date":"2021-03"},"lastUpdatePostDateStruct":{"date":"2025-12-01"}},
  "designModule":{"phases":["PHASE4"],"enrollmentInfo":{"count":60,"type":"ESTIMATED"}},
  "eligibilityModule":{"sex":"ALL","minimumAge":"6 Months","maximumAge":"17 Years"},
  "conditionsModule":{"conditions":["Juvenile Idiopathic Arthritis","Rheumatoid Arthritis"]},
  "armsInterventionsModule":{"interventions":[{"type":"BIOLOGICAL","name":"Adalimumab"}]}}}$j$, '2026-01-05', '2026-01-06 02:00+00'),

-- behavioural trial: in dim_trial but no drug rows
('NCT00000005', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000005","briefTitle":"Mindfulness for Postpartum Depression"},
  "statusModule":{"overallStatus":"NOT_YET_RECRUITING","startDateStruct":{"date":"2026-09-01"},"lastUpdatePostDateStruct":{"date":"2025-10-15"}},
  "designModule":{"enrollmentInfo":{"count":40,"type":"ESTIMATED"}},
  "eligibilityModule":{"sex":"FEMALE","minimumAge":"18 Years","maximumAge":"45 Years"},
  "conditionsModule":{"conditions":["Postpartum Depression"]},
  "armsInterventionsModule":{"interventions":[{"type":"BEHAVIORAL","name":"Mindfulness training"}]}}}$j$, '2026-01-05', '2026-01-06 02:00+00');
