-- =====================================================================
-- Bronze tables: the contract between Airflow ingestion (2.1) and dbt (2.3).
--
-- Design notes
--   * One row per API record. The response is stored unchanged as JSONB, so a
--     change in the API's structure cannot break ingestion (schema-on-read).
--   * The natural key is extracted into its own column at insert time so it can be
--     indexed and checked without parsing JSON.
--   * load_date is the Airflow logical date of the run (the API date window). It is
--     the unit of idempotency: a re-run deletes its own load_date and re-inserts.
--   * ingested_at is the incremental watermark used by gold.fact_adverse_event.
--   * No primary key on the natural key: a report can legitimately arrive several
--     times (follow-up versions). Deduplication to the latest version happens in silver.
-- Idempotent: safe to run on every container start.
-- =====================================================================

create table if not exists bronze.faers_reports (
    safetyreportid text        not null,
    payload        jsonb       not null,
    load_date      date        not null,
    ingested_at    timestamptz not null default now()
);

comment on table  bronze.faers_reports is
    'One row per openFDA FAERS safety report per load; payload is one element of the API results array.';
comment on column bronze.faers_reports.load_date is
    'Airflow logical date of the run that loaded the row; unit of idempotent reload.';
comment on column bronze.faers_reports.ingested_at is
    'Insert timestamp; incremental watermark for gold.fact_adverse_event.';

create table if not exists bronze.ct_studies (
    nct_id      text        not null,
    payload     jsonb       not null,
    load_date   date        not null,
    ingested_at timestamptz not null default now()
);

comment on table  bronze.ct_studies is
    'One row per ClinicalTrials.gov study per load; payload is one element of the API studies array.';

-- ---------------------------------------------------------------------
-- Indexes
--   load_date   : the idempotent DELETE of a re-run, and partition-style pruning
--   ingested_at : the incremental read by dbt (ingested_at > watermark)
--   natural key : deduplication to the latest version in silver (window partition)
-- ---------------------------------------------------------------------
create index if not exists ix_faers_load_date   on bronze.faers_reports (load_date);
create index if not exists ix_faers_ingested_at on bronze.faers_reports (ingested_at);
create index if not exists ix_faers_report_id   on bronze.faers_reports (safetyreportid);

create index if not exists ix_ct_load_date      on bronze.ct_studies (load_date);
create index if not exists ix_ct_ingested_at    on bronze.ct_studies (ingested_at);
create index if not exists ix_ct_nct_id         on bronze.ct_studies (nct_id);

-- ---------------------------------------------------------------------
-- Ingestion audit log: one row per (DAG run, source), written by the loader.
-- Lets the team see what each run loaded and lets a DAG skip work that is already done.
-- ---------------------------------------------------------------------
create table if not exists bronze.ingestion_log (
    log_id        bigserial   primary key,
    source_name   text        not null,
    load_date     date        not null,
    dag_run_id    text,
    rows_deleted  integer     not null default 0,
    rows_inserted integer     not null default 0,
    status        text        not null default 'success'
                  check (status in ('success', 'failed')),
    message       text,
    loaded_at     timestamptz not null default now()
);

create index if not exists ix_ingestion_log_source_date
    on bronze.ingestion_log (source_name, load_date);
