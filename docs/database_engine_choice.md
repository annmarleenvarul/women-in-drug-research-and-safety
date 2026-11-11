# Database engine choice: PostgreSQL

Source material for the report section on database-engine justification. Numbers in
*Workload* should be replaced with the real figures once a full ingestion run has finished.

## Decision

PostgreSQL 16 holds all three medallion layers (`bronze`, `silver`, `gold`) as schemas in a
single database, running in Docker next to Airflow. Airflow's own metadata lives in a separate
database, so a warehouse rebuild cannot disturb the scheduler.

## Workload

| Property | This project | Consequence |
|---|---|---|
| Volume | ~10^5–10^6 fact rows (FAERS reports × suspect drugs × reactions) | Fits comfortably in one node's memory and disk; no distributed engine needed |
| Raw format | Nested JSON from two REST APIs | The engine must store and query documents natively |
| Write pattern | Daily batch; delete + insert per load date; SCD Type 2 updates | Row-oriented storage with cheap deletes and updates |
| Read pattern | Star-schema joins, grouped aggregates over a handful of columns, 5 analytical queries | Columnar would be faster, but at this size the difference is milliseconds |
| Consistency | A failed load must not leave the warehouse half-updated | Transactional DDL and DML |
| Deployment | Local Docker, five contributors, one semester | Smallest possible operational footprint |

## Why PostgreSQL fits

1. **JSONB covers the semi-structured sources.** openFDA and ClinicalTrials.gov return deeply
   nested JSON. Bronze stores each record unchanged as `jsonb`, so a change in the API's shape
   cannot break ingestion; dbt flattens it in silver with `jsonb_array_elements`. A separate
   document store would add a second engine for no gain at this size.
2. **Transactional DML suits the write pattern.** The idempotent load is `DELETE` + `INSERT`
   per `load_date` in one transaction; the incremental fact uses dbt's `delete+insert`; the
   SCD Type 2 snapshot updates `dbt_valid_to` on existing rows. All three are cheap row-level
   operations in a row store and awkward in engines built for append-only data.
3. **Mature dbt support.** `dbt-postgres` is the reference adapter, including snapshots and
   index configuration, so no adapter-specific workarounds are needed.
4. **One engine, one container.** Airflow, dbt and any BI tool all speak the same protocol, which
   matters more for a five-person student project than raw scan speed.

## Comparison with a columnar engine (DuckDB / ClickHouse)

Columnar engines store each column separately and compress it, so an aggregate that touches 3 of
12 columns reads roughly a quarter of the data and runs vectorised over it. For a scan-heavy
query such as "serious-outcome rate by sex and age band over all reports", that is a real
advantage, and it grows with volume.

It does not pay off here:

- **Scale.** At ~10^6 rows the gold tables are a few hundred megabytes; PostgreSQL answers the
  five analytical queries in well under a second with the indexes in `dbt_project.yml`.
  Columnar formats win when scans are large enough for I/O to dominate.
- **Update pattern.** The pipeline deletes and rewrites rows by report and closes SCD Type 2
  versions. ClickHouse treats updates as asynchronous mutations rather than ordinary statements,
  which would complicate the Type 2 dimension. DuckDB handles updates but is single-writer and
  file-based, which fits a local analysis better than a database several containers share.
- **Concurrency and deployment.** DuckDB has no server process, so Airflow tasks and a BI tool
  cannot write and read at the same time without extra coordination. ClickHouse would be a second
  service to operate and a second dialect to maintain in dbt.

Put briefly: the advantages of a columnar engine scale with data volume and scan width, while its
costs land on exactly the operations this pipeline performs most often.

## Trade-offs accepted

- Aggregations over the full fact table will be slower than a columnar engine once the data grows
  by an order of magnitude. The mitigation would be pre-aggregated gold tables, or moving the gold
  layer to DuckDB while keeping bronze in PostgreSQL.
- `jsonb` is larger on disk than typed columns and cannot be compressed per column. This is
  accepted for bronze, where fidelity to the source matters more than size; silver and gold are
  typed.
