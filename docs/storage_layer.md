# Storage layer (project section 2.2)

What this part owns, and the contract it offers the other two parts.

## Layout

```
warehouse/
  docker-compose.yml     PostgreSQL 16 service on the shared `project_net` network
  init/00_init.sh        runs once on first start: creates the app user, applies the DDL
  ddl/01_schemas.sql     bronze / silver / gold schemas
  ddl/02_bronze_tables.sql  bronze tables, indexes, ingestion_log
  ddl/03_roles.sql       optional least-privilege roles for Airflow and dbt
  Makefile               up / down / reset / psql / ddl / sample
ingestion/
  config.py              WarehouseConfig.from_env()
  warehouse.py           load_bronze(), log_failure(), bronze_row_count()
  quality.py             run_bronze_checks()
docs/
  database_engine_choice.md
```

All DDL is idempotent (`create ... if not exists`), so re-running it on a live database is safe.

## Medallion layers

| Layer | Schema | Owner | Materialisation |
|---|---|---|---|
| Bronze | `bronze` | Airflow (this part's DDL and loader) | Tables with a `jsonb` payload, as received |
| Silver | `silver` | dbt | Tables (`stg_*`) plus the SCD Type 2 snapshot |
| Gold | `gold` | dbt | Star schema: `fact_adverse_event` incremental, the rest full-refresh tables |

Keeping the layers in separate schemas means a reader can tell at a glance which layer a table
belongs to, and the two owners never write to the same schema.

## Contract for the ingestion DAGs (2.1)

```python
from datetime import date
from ingestion import load_bronze, run_bronze_checks, log_failure

# inside a @task
stats = load_bronze(
    "faers",                       # or "clinicaltrials"
    records,                       # list of dicts straight from the API response
    load_date=context["data_interval_start"].date(),
    dag_run_id=context["run_id"],
)
run_bronze_checks("faers", load_date)   # raises DataQualityError -> the DAG stops before dbt
```

`load_bronze` deletes the rows of that `load_date` and inserts the new ones in **one
transaction**, so re-running a DAG for the same date replaces its rows instead of duplicating
them, and a crash mid-load leaves the previous contents intact. Every call writes a row to
`bronze.ingestion_log` (rows deleted, rows inserted, run id, status), which is the evidence for
the idempotency requirement in the README and the report.

Adding a third source means adding one entry to `SOURCES` in `ingestion/warehouse.py` and one
`create table` to `ddl/02_bronze_tables.sql`.

## Physical design

**Bronze** indexes `load_date` (the idempotent delete), `ingested_at` (dbt's incremental read)
and the natural key (deduplication in silver). No primary key on the natural key: a report may
legitimately arrive several times as follow-up versions, and silver keeps the latest.

**Gold** indexes are declared per model in `dbt_project.yml` under `+indexes`, so dbt recreates
them whenever it rebuilds a table. Every surrogate key has a unique index; every fact foreign key
has one; the attributes the analytical queries group by (`sex, age_band`, `year, quarter`,
`nct_id, is_current`) have composite indexes. `fact_adverse_event` additionally indexes
`report_id` and `ingested_at`, which drive the incremental delete+insert.

Incremental and SCD Type 2 support is in the model layer: `fact_adverse_event` is incremental on
`report_id`, and `dim_trial` comes from a dbt snapshot keyed on the registry's last-update date.

## Running it

```bash
cp .env.example .env            # then fill in the passwords
make -f warehouse/Makefile up   # starts PostgreSQL and applies the DDL
make -f warehouse/Makefile sample   # optional: sample bronze data for development
```

Checks:

```sql
select tablename from pg_tables where schemaname = 'bronze';
select * from bronze.ingestion_log order by loaded_at desc limit 10;
```
