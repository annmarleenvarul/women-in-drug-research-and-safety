# Raw → Silver → Gold

The authoritative ingestion is `dags/ingest_sources.py`, with tables defined in
`sql/create_raw_tables.sql`. All layers live in the existing `research-db` service,
`women_research` database. There is no Bronze layer in this pipeline.

Raw tables:
- `raw.faers_reports(report_id, report_version, payload, batch_id, loaded_at)`
- `raw.ctgov_studies(nct_id, source_updated_date, payload, batch_id, loaded_at)`

Ingestion keeps its existing `ON CONFLICT (ID, version) DO NOTHING` behavior. Silver
selects the highest numeric FAERS version (then load time and version text), and the
latest ClinicalTrials.gov source update date. Downstream `ingested_at` columns are
aliases of Raw `loaded_at`; they do not require another Raw column.

Airflow runs these gates with the default `all_success` trigger rule:

1. Both API ingestion tasks.
2. Existing `check_tables` minimum distinct-ID counts (1,000 per source).
3. `dbt test --select source:raw`: source keys, required fields and payload consistency.
4. `dbt build --select path:models/silver --indirect-selection eager`.
5. `dbt build --select path:models/gold snap_ct__studies --indirect-selection eager`.

Every test failure exits nonzero and blocks the next task. The age-range test is now
an error as well. dbt is installed in `/opt/dbt`, separate from Airflow's dependencies.
The project mount is read-only; target files and logs use writable Docker volumes.

Gold dimensions and facts are rebuilt from validated Silver tables. Rebuilding the
adverse-event fact also removes old rows when a corrected report has no suspect drugs
or reactions left. Stable hash keys and distinct model grains make reruns safe.
The timestamp snapshot retains trial versions observed across successful runs; an
unchanged source update date creates no new snapshot version. A first run does not
replay every historical version already present in Raw.

## Complete pipeline

From the repository root:

```bash
cp .env.example .env  # only if .env does not already exist
# Set credentials and OPENFDA_API_KEY; the existing ingestion requires that key.
docker compose config --quiet
docker compose build
docker compose up -d
docker compose ps -a
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow tasks list ingest_sources
docker compose exec airflow-scheduler airflow dags unpause ingest_sources
docker compose exec airflow-scheduler airflow dags trigger ingest_sources
```

Open Airflow at http://localhost:8085 (or AIRFLOW_WEB_PORT from .env). Monitor
`ingest_sources` and confirm every gate succeeds. The schedule remains manual and each
API task still requests exactly 1,000 records, without pagination or date windows.

Expected result: all six tasks succeed: `ingest_ctgov`, `ingest_faers`,
`check_tables`, `test_raw`, `build_silver`, and `build_gold`. `airflow-init`
exiting with code 0 is normal. The scheduler and webserver should use
`women-drug-airflow:local`; restarting an old container alone does not apply a new image.

Verify the resulting tables and counts:

```bash
docker compose exec -T research-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('raw', 'silver', 'gold')
ORDER BY 1, 2;
SELECT count(*) FROM gold.fact_adverse_event;
SELECT count(*) FROM gold.fact_trial_drug;
SELECT count(*) FROM gold.dim_patient_profile;
SQL
```

Expect 2 Raw tables, 6 Silver staging tables, 1 Silver snapshot, and 9 Gold tables.
`dim_patient_profile` contains 15 rows; fact counts depend on the API responses.
The DAG uses real API responses directly. Saved sample files are not required.
The dbt tests are live data-quality gates and remain part of every run.

For an existing database volume that predates the Raw tables, apply the authoritative
DDL without deleting data:

```bash
docker compose exec -T research-db sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/create_raw_tables.sql
```

To check dbt separately inside the running scheduler:

```bash
docker compose exec airflow-scheduler /opt/dbt/bin/dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target-path /opt/airflow/dbt-target --log-path /opt/airflow/dbt-logs
```

For local dbt, install `requirements.txt`, export the `RESEARCH_DB_*` variables from
.env, set `RESEARCH_DB_HOST=127.0.0.1` and use the external `RESEARCH_DB_PORT`.
Inside Compose, the host is `research-db` and the port is always 5432.

Quality gates prevent downstream tasks from starting, but an entire dbt build is not
one database transaction. A Gold failure can leave a mix of updated and previous Gold
tables; rerun the task after correcting the problem.
