# Convenience targets. Run from the repository root with: make -f warehouse/Makefile <target>
include .env
export

up:        ## start the warehouse
	docker compose -f warehouse/docker-compose.yml --env-file .env up -d

down:      ## stop it (data is kept in the volume)
	docker compose -f warehouse/docker-compose.yml --env-file .env down

reset:     ## stop it and DELETE all data
	docker compose -f warehouse/docker-compose.yml --env-file .env down -v

psql:      ## open a shell on the warehouse
	docker exec -it wds_postgres psql -U $(WAREHOUSE_USER) -d $(WAREHOUSE_DB)

ddl:       ## re-apply the DDL to a running database (idempotent)
	cat warehouse/ddl/01_schemas.sql warehouse/ddl/02_bronze_tables.sql | \
	docker exec -i wds_postgres psql -U $(WAREHOUSE_USER) -d $(WAREHOUSE_DB)

sample:    ## load sample bronze data for development
	docker exec -i wds_postgres psql -U $(WAREHOUSE_USER) -d $(WAREHOUSE_DB) < dbt/scripts/sample_bronze.sql

.PHONY: up down reset psql ddl sample
