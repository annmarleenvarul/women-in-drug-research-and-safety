#!/bin/bash
# Runs once, the first time the Postgres volume is created.
# Creates the application user and applies the medallion DDL.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
do \$\$ begin
    if not exists (select 1 from pg_roles where rolname = '${WAREHOUSE_USER}') then
        execute format('create role %I login password %L', '${WAREHOUSE_USER}', '${WAREHOUSE_PASSWORD}');
    end if;
end \$\$;
grant all privileges on database ${POSTGRES_DB} to ${WAREHOUSE_USER};
SQL

for file in /ddl/*.sql; do
    # 03_roles.sql needs psql variables and is optional; skip it in the simple setup
    case "$(basename "$file")" in 03_roles.sql) continue ;; esac
    echo "applying $file"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$file"
done

# The application user owns the layers it will create objects in
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
alter schema bronze owner to ${WAREHOUSE_USER};
alter schema silver owner to ${WAREHOUSE_USER};
alter schema gold   owner to ${WAREHOUSE_USER};
alter table bronze.faers_reports owner to ${WAREHOUSE_USER};
alter table bronze.ct_studies    owner to ${WAREHOUSE_USER};
alter table bronze.ingestion_log owner to ${WAREHOUSE_USER};
SQL
