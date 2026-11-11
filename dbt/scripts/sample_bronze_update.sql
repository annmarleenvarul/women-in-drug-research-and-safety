-- =====================================================================
-- Simulates the NEXT ingestion run, to demonstrate:
--   * SCD Type 2: NCT00000001 changes status RECRUITING -> COMPLETED
--   * Incremental fact load with a corrected report version
--     (report 10000001 v2 no longer lists NAUSEA -> the stale fact row must disappear)
--   * a brand-new report
-- Run after a first `dbt build`, then run `dbt build` again.
-- =====================================================================
insert into bronze.ct_studies (nct_id, payload, load_date, ingested_at) values
('NCT00000001', $j${"protocolSection":{
  "identificationModule":{"nctId":"NCT00000001","briefTitle":"Metformin in Early Breast Cancer"},
  "statusModule":{"overallStatus":"COMPLETED","startDateStruct":{"date":"2022-04-15"},"lastUpdatePostDateStruct":{"date":"2026-01-20"}},
  "designModule":{"phases":["PHASE2"],"enrollmentInfo":{"count":114,"type":"ACTUAL"}},
  "eligibilityModule":{"sex":"ALL","minimumAge":"18 Years","maximumAge":"75 Years"},
  "conditionsModule":{"conditions":["Breast Cancer"]},
  "armsInterventionsModule":{"interventions":[{"type":"DRUG","name":"Metformin 500 mg tablets"},{"type":"DRUG","name":"Placebo oral tablet"}]}}}$j$, '2026-01-21', '2026-01-22 02:00+00');

insert into bronze.faers_reports (safetyreportid, payload, load_date, ingested_at) values
('10000001', $j${"safetyreportid":"10000001","safetyreportversion":"2","receivedate":"20260105","serious":"1","seriousnesshospitalization":"1",
  "patient":{"patientsex":"2","patientonsetage":"52","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"GLUCOPHAGE","openfda":{"generic_name":["METFORMIN HYDROCHLORIDE"],"rxcui":["861007"],"pharm_class_epc":["Biguanide [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Lactic acidosis"}]}}$j$, '2026-01-21', '2026-01-22 02:00+00'),

('10000007', $j${"safetyreportid":"10000007","safetyreportversion":"1","receivedate":"20260121","serious":"2",
  "patient":{"patientsex":"2","patientonsetage":"34","patientonsetageunit":"801",
   "drug":[{"drugcharacterization":"1","medicinalproduct":"METFORMIN HCL ER","openfda":{"generic_name":["METFORMIN HYDROCHLORIDE"],"rxcui":["860975"],"pharm_class_epc":["Biguanide [EPC]"]}}],
   "reaction":[{"reactionmeddrapt":"Nausea"},{"reactionmeddrapt":"Diarrhoea"}]}}$j$, '2026-01-21', '2026-01-22 02:00+00');
