-- =====================================================================
-- Medallion layers as separate PostgreSQL schemas.
--   bronze : raw API payloads, written by Airflow (this file owns these tables)
--   silver : cleaned / flattened models, created and owned by dbt
--   gold   : star schema, created and owned by dbt
-- Only bronze is created by hand: dbt creates silver and gold objects itself,
-- but the schemas are created here so permissions can be granted up front.
-- Idempotent: safe to run on every container start.
-- =====================================================================

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

comment on schema bronze is 'Raw ingested data, loaded by Airflow. Never edited by hand.';
comment on schema silver is 'Cleaned, deduplicated, flattened data. Managed by dbt.';
comment on schema gold   is 'Dimensional star schema for analysis. Managed by dbt.';
