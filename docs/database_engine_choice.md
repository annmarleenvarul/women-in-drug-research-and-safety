# Database engine choice: PostgreSQL 16

## Decision

PostgreSQL 16 (`research-db` in [`compose.yml`](../compose.yml)) holds all three medallion
layers as schemas in one database: `raw` (bronze), `silver` and `gold`. Airflow's metadata
lives in a separate PostgreSQL container (`airflow-db`), so rebuilding the warehouse cannot
disturb the scheduler.

## Workload

| Property | This project | Consequence |
|---|---|---|
| Volume | Per run: up to 1,000 documents per source. After two runs: 2,000 FAERS reports -> 19,429 `fact_adverse_event` rows; 1,997 trials -> 3,355 `fact_trial_drug` rows. The whole warehouse is about 60 MB, 43 MB of it raw JSON | Fits in one node's memory; no distributed engine needed |
| Raw format | Nested JSON from two REST APIs | The engine must store and query documents natively (`JSONB`) |
| Write pattern | Batch loads; append-only `INSERT ... ON CONFLICT DO NOTHING` in raw; the SCD Type 2 snapshot updates `dbt_valid_to` on changed trials; silver and gold tables are rebuilt inside a transaction each run | Cheap row-level updates and transactional table swaps |
| Read pattern | Star-schema joins and grouped aggregates over a few columns (the five business questions) | Columnar would be faster at scale; at this size the difference is milliseconds |
| Consistency | A failed load must not leave raw half-written | Transactional DML |
| Deployment | Local Docker Compose, five contributors, Airflow and dbt in separate containers | One server process that several clients share |

## Why PostgreSQL fits

1. **JSONB covers the semi-structured sources.** Bronze keeps each API record unchanged as
   `jsonb`, and dbt flattens it in silver with `jsonb_array_elements` and SQL/JSON paths. A
   separate document store would add a second engine for no gain at this size. See
   [semi_structured_integration.md](semi_structured_integration.md).
2. **Transactional, row-level DML suits the write pattern.** The idempotent raw load and the
   SCD Type 2 snapshot are cheap row-level operations in a row store, and awkward in engines
   built for append-only data. dbt rebuilds each silver/gold table under a temporary name and
   swaps it in within one transaction, so readers never see a half-built table.
3. **Mature dbt support.** `dbt-postgres` supports snapshots, transactional table rebuilds and
   the index configuration in [`dbt_project.yml`](../dbt/dbt_project.yml) without workarounds.
4. **A shared server.** The Airflow tasks, dbt and pgAdmin connect to the same service at the
   same time, which matters more for this project than raw scan speed.

## Comparison with a columnar engine (DuckDB / ClickHouse)

Columnar engines store each column separately and compress it, so an aggregate that touches 3 of
12 columns reads about a quarter of the data and runs vectorised. For a scan-heavy query such as
"serious-outcome rate by sex and age band over all reports" (Q5), that advantage is real and grows
with volume. It does not pay off here:

- **Scale.** The gold layer is about 11 MB; PostgreSQL answers the five queries in well under a
  second with B-tree indexes on the surrogate keys. Columnar storage wins when scans are large
  enough for I/O to dominate.
- **Update pattern.** The pipeline closes SCD Type 2 trial versions with row-level updates and
  skips already-loaded raw documents by primary key. ClickHouse runs updates as asynchronous mutations, which complicates the Type 2
  dimension. DuckDB handles updates but is a single-writer embedded file, which fits a local
  analysis better than a database that several containers share.
- **Deployment.** DuckDB has no server process, so Airflow tasks and a BI tool cannot write and
  read at the same time without extra coordination. ClickHouse would be a second service to run
  and a second SQL dialect to maintain in dbt.

The advantages of a columnar engine grow with data volume and scan width, while its costs fall on
exactly the operations this pipeline performs most often.

## Trade-offs accepted

- Aggregations over the full fact table will be slower than in a columnar engine once the data
  grows by orders of magnitude (the full FAERS history is tens of millions of reports). The
  mitigation would be pre-aggregated gold tables, or a DuckDB gold layer reading from PostgreSQL.
- `jsonb` is larger on disk than typed columns: raw is about 23x the size of the silver
  staging tables. This is accepted for bronze, where fidelity to the source matters more than
  size; silver and gold are typed.
