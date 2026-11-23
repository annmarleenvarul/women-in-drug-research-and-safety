# Extra credit B: semi-structured data integration

Both of our sources are **JSON REST APIs** with deeply nested, partly optional, versioned
records: openFDA FAERS adverse-event reports and ClinicalTrials.gov API v2 studies. Instead of
flattening them in Python at ingestion time, the pipeline lands every record **unchanged as a
PostgreSQL `JSONB` document** in the bronze layer and does all structural work (navigating
nested objects, exploding arrays, decoding codes, choosing between alternative fields) in dbt.

This document shows how the records are extracted, how their structure is handled, how they
become the star schema, and how this compares with storing the same data in conventional
PostgreSQL tables. All numbers come from the analyses in
[`dbt/analyses/semi_structured/`](../dbt/analyses/semi_structured/); full output is in
[`docs/results/semi_structured.md`](results/semi_structured.md) (run of 2026-11-23: 2,000 FAERS
reports and 1,997 trials).

## 1. Extraction: API -> bronze JSONB

[`dags/ingest_sources.py`](../dags/ingest_sources.py) calls each API with the DAG parameters
(date window, record limit) and stores each record as one row:

| Column | FAERS (`raw.faers_reports`) | ClinicalTrials.gov (`raw.ctgov_studies`) |
|---|---|---|
| natural key | `report_id` = `safetyreportid` | `nct_id` = `protocolSection.identificationModule.nctId` |
| version | `report_version` = `safetyreportversion` (SHA-256 of the document if absent) | `source_updated_date` = `lastUpdatePostDateStruct.date` |
| document | `payload JSONB`, the whole report | `payload JSONB`, the whole study |
| lineage | `batch_id` (Airflow run ID), `loaded_at` | same |

Python extracts **only the key and version**, because they form the primary key that makes the
load idempotent (`ON CONFLICT (id, version) DO NOTHING`). Everything else stays inside the
document. A new version of a report or study is a new row; an identical rerun is a no-op.
Two dbt tests ([`assert_raw_faers_payload`](../dbt/tests/assert_raw_faers_payload.sql),
[`assert_raw_ctgov_payload`](../dbt/tests/assert_raw_ctgov_payload.sql)) check that the
extracted key columns agree with the values inside the JSON.

## 2. Handling the structure (silver, dbt)

| Structural feature | Evidence | How silver handles it |
|---|---|---|
| **Nested objects**, 3-4 levels deep | `payload.patient.drug[].openfda.generic_name[]` | PostgreSQL path operators `->`, `->>`, `#>>` and SQL/JSON paths (`jsonb_path_exists`, `jsonb_path_query_array`) |
| **Arrays to rows** | S2: a FAERS report has 1-153 drugs (avg 3.4) and 1-101 reactions (avg 2.8); a trial has 1-45 conditions and 1-21 interventions | `cross join lateral jsonb_array_elements(...)` in [`stg_faers__suspect_drugs`](../dbt/models/silver/stg_faers__suspect_drugs.sql), [`stg_faers__reactions`](../dbt/models/silver/stg_faers__reactions.sql), [`stg_ct__study_conditions`](../dbt/models/silver/stg_ct__study_conditions.sql), [`stg_ct__study_drugs`](../dbt/models/silver/stg_ct__study_drugs.sql) |
| **Optional fields** | S1: patient age present in 63.8 % of reports, sex 90.0 %, trial `maximumAge` 55.1 %; seriousness flags only present when true (46-54 %) | `coalesce(..., '[]')` before exploding arrays; missing values become `NULL` / `Unknown` rather than failing the load |
| **Same fact in different places** | S3: a suspect drug's ingredient comes from `openfda.generic_name` in 81.2 % of cases, `activesubstance` 8.5 %, free-text `medicinalproduct` 10.2 % | `coalesce` over the three paths, then the [`normalize_drug_name`](../dbt/macros/normalize_drug_name.sql) macro |
| **Values encoded as strings** | S4: `patientsex` `"1"/"2"`, `serious` `"1"`, `receivedate` `"20250101"`, ages `"18 Years"`, start dates `"2024-03"` (528 studies) vs `"2024-03-15"` (1,460) | decoding `case` expressions, [`age_helpers`](../dbt/macros/age_helpers.sql) and [`date_helpers`](../dbt/macros/date_helpers.sql) macros |
| **Multiple versions** of a document | follow-up FAERS versions; studies updated over time | [`base_faers__latest_reports`](../dbt/models/silver/base_faers__latest_reports.sql) and [`base_ct__latest_studies`](../dbt/models/silver/base_ct__latest_studies.sql) keep the latest; the snapshot `snap_ct__studies` keeps trial history (SCD Type 2) |
| **Schema drift** (API renames a field) | n/a | the 29 paths silver depends on are listed once in [`semi_structured_paths`](../dbt/macros/semi_structured_paths.sql); [`warn_raw_payload_schema_drift`](../dbt/tests/warn_raw_payload_schema_drift.sql) runs in the DAG's `test_raw` task and warns if a required path is absent from every document |

## 3. From one document to the star schema

A FAERS report with 2 suspect drugs and 3 reactions:

```text
raw.faers_reports            1 row   payload = { safetyreportid, receivedate, serious, ...,
                                                 patient: { patientsex, patientonsetage,
                                                            drug: [d1, d2, d3(concomitant)],
                                                            reaction: [r1, r2, r3] } }
        |
silver.stg_faers__reports         1 row   report-level fields decoded (sex, age band, flags, date)
silver.stg_faers__suspect_drugs   2 rows  drug[] exploded, drugcharacterization = "1" kept, name normalised
silver.stg_faers__reactions       3 rows  reaction[] exploded, MedDRA term standardised
        |
gold.fact_adverse_event           6 rows  2 suspect drugs x 3 reactions (fact grain)
  -> dim_drug, dim_reaction, dim_patient_profile, dim_date via surrogate keys
```

Across the whole load (S2), **2,000 FAERS documents become 19,429 fact rows** (median 2 per
report, but up to 4,949 for one report listing many drugs and reactions), and **1,997 trial
documents become 3,355 `fact_trial_drug` rows and 3,536 `bridge_trial_condition` rows**. This
skew is why the analytical queries count `DISTINCT report_id`.

## 4. Comparison with conventional PostgreSQL tables

The alternative is to define typed relational tables up front (for example `report`,
`report_drug`, `report_reaction`, `trial`, `trial_condition`, `trial_intervention`) and have
the Python ingestion code parse each JSON document into them (schema-on-write). Both approaches
run on the same PostgreSQL engine and feed the same gold star schema; the difference is
**where and when the JSON structure is interpreted**.

| Aspect | JSONB bronze + dbt flattening (ours) | Conventional tables at ingestion |
|---|---|---|
| When the schema is applied | On read, in version-controlled, tested dbt SQL | On write, in Python ingestion code |
| New question needs a field we did not extract | Already stored: 26 top-level keys are kept but unused (S5), e.g. `seriousnessdisabling` (911 of 2,000 reports) and `seriousnesscongenitalanomali` (900), both relevant to Q5. Add a column in dbt and rebuild | Change the DDL, change the parser, **re-call the API** for history. Older FAERS versions may no longer be served |
| Fixing a parsing bug | Fix the model and `dbt build`; raw data is untouched | Data was lost or mis-parsed at load; needs re-ingestion |
| API adds or renames a field | Load still succeeds; the drift test warns; fix in SQL | Insert fails (strict parser), or the field is silently dropped (lenient parser) |
| Ingestion code | Small: key + version + whole document | Large: nested loops, type casts, fallbacks and decoding in Python, harder to test |
| Type safety and constraints at load | Weak: only the key columns are typed; types are enforced later (silver casts, dbt tests) | Strong: typed columns, `NOT NULL`, foreign keys reject bad data immediately |
| Storage | Bronze is **43 MB** (about 10 KB compressed per document), about 23x the 1.8 MB of the six silver staging tables that hold every field we use | Roughly the silver size; nothing unused is stored |
| Query cost | Each dbt run re-parses the JSON (full scans with `jsonb_array_elements`); fine at thousands of documents, but grows with volume | Cheaper scans and typed columns; standard B-tree indexes |
| Analysts' view (gold) | Identical: a typed star schema | Identical |

**Why it fits this project.** Both APIs are external, versioned and outside our control, the
business questions are still evolving (for example, adding disability outcomes to Q5), and the
volume is small (thousands of documents per run). Keeping the full document makes the warehouse
replayable: any silver or gold table can be rebuilt from bronze without calling the API again,
and the structure-handling logic lives in one tested place (dbt) instead of being split between
Python and SQL. The type safety we give up at load time is recovered one layer later: silver
casts every field and the dbt tests act as the quality gate before gold is refreshed.

**When conventional tables would be better.** With very high volumes (the full FAERS history
has tens of millions of reports), bronze storage and repeated JSON parsing would dominate. An
incremental silver layer, or extracting the frequently used paths into typed or generated
columns at load, would then pay off. A `GIN` index on `payload` would not help our workload,
because dbt reads every document in a batch rather than searching for specific keys.

**Why not MongoDB.** A document store would hold the same JSON, but it would add a second
database and an extra extract step before dbt, which cannot transform MongoDB directly.
PostgreSQL `JSONB` gives document storage and the relational star schema in one engine, inside
the same transaction and with the same dbt adapter.

## Reproduce

```powershell
docker compose exec airflow-scheduler /opt/dbt/bin/dbt compile --select path:analyses/semi_structured --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target-path /opt/airflow/dbt-target --log-path /opt/airflow/dbt-logs
Get-Content scripts/run_analyses.py -Raw | docker compose exec -T airflow-scheduler python - semi_structured
docker compose cp airflow-scheduler:/opt/airflow/dbt-target/results/semi_structured.md docs/results/
```
