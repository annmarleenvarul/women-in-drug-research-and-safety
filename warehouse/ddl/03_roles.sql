-- =====================================================================
-- Optional: least-privilege roles, for the "configuration not hard-coding" guidance.
-- Passwords come from environment variables, never from this file:
--
--   psql -v airflow_pw="$AIRFLOW_DB_PASSWORD" -v dbt_pw="$DBT_DB_PASSWORD" \
--        -d warehouse -f warehouse/ddl/03_roles.sql
--
-- Skip this file for a simple local setup where one user owns everything
-- (warehouse/init/00_init.sh skips it by name).
--
-- Note: the CREATE ROLE statements are generated with \gexec rather than a DO block,
-- because psql does not substitute :variables inside dollar-quoted strings.
-- =====================================================================

-- Airflow writes bronze only.
select format('create role airflow_ingest login password %L', :'airflow_pw')
where not exists (select 1 from pg_roles where rolname = 'airflow_ingest')
\gexec

grant usage on schema bronze to airflow_ingest;
grant select, insert, delete on all tables in schema bronze to airflow_ingest;
grant usage, select on all sequences in schema bronze to airflow_ingest;
alter default privileges in schema bronze
    grant select, insert, delete on tables to airflow_ingest;
alter default privileges in schema bronze
    grant usage, select on sequences to airflow_ingest;

-- dbt reads bronze and owns silver and gold.
select format('create role dbt_transform login password %L', :'dbt_pw')
where not exists (select 1 from pg_roles where rolname = 'dbt_transform')
\gexec

grant usage on schema bronze to dbt_transform;
grant select on all tables in schema bronze to dbt_transform;
alter default privileges in schema bronze grant select on tables to dbt_transform;
grant create, usage on schema silver, gold to dbt_transform;
