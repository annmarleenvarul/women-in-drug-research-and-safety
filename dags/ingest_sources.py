"""Ingest ClinicalTrials.gov and openFDA FAERS into the raw (bronze) layer, then run dbt.

Parameters (set in the "Trigger DAG" form):
  ctgov_updated_since   ClinicalTrials.gov studies last updated on/after this date
  faers_received_from   FAERS reports received by the FDA on/after this date
  faers_received_to     FAERS reports received by the FDA on/before this date
  record_limit          maximum records fetched per source (one API page, 1-1000)

Both API queries are sorted by date, so the same parameters fetch the same records.
Loads use ON CONFLICT DO NOTHING on the natural key, so reruns never create duplicates.
"""
import hashlib
import json
import os
from contextlib import closing
from datetime import date, datetime, timedelta

import requests
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json


CONN_ID = "women_db"
MAX_PAGE_SIZE = 1000  # largest single page both APIs return


def load_rows(table, id_column, version_column, rows):
    """Insert (id, version, payload, batch_id) rows; existing keys are skipped."""
    if not rows:
        raise ValueError(f"The API returned no records for raw.{table}; check the date parameters.")

    keys = [(row[0], str(row[1])) for row in rows]
    if len(set(keys)) != len(keys):
        raise ValueError("Input contains duplicate record IDs.")

    sql = f"""
        INSERT INTO raw.{table} ({id_column}, {version_column}, payload, batch_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ({id_column}, {version_column}) DO NOTHING
    """

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    inserted = 0

    with closing(pg.get_conn()) as connection:
        with connection:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(sql, row)
                    inserted += cursor.rowcount

    print(f"Received: {len(rows)}")
    print(f"Inserted: {inserted}")
    print(f"Skipped (already loaded): {len(rows) - inserted}")

    # Passed to check_tables via XCom to confirm every fetched record is in raw.
    return keys


def ingest_ctgov(**context):
    params = context["params"]
    updated_since = params["ctgov_updated_since"]
    query = (
        "AREA[StudyType]INTERVENTIONAL"
        " AND (AREA[InterventionType]DRUG OR AREA[InterventionType]BIOLOGICAL)"
        f" AND AREA[LastUpdatePostDate]RANGE[{updated_since},MAX]"
    )

    response = requests.get(
        "https://clinicaltrials.gov/api/v2/studies",
        params={
            "filter.advanced": query,
            "sort": "LastUpdatePostDate:asc",
            "pageSize": params["record_limit"],
            "format": "json",
        },
        timeout=60,
    )
    response.raise_for_status()

    rows = []
    for study in response.json()["studies"]:
        protocol = study.get("protocolSection", {})
        nct_id = protocol.get("identificationModule", {}).get("nctId")
        updated_date = protocol.get("statusModule", {}).get("lastUpdatePostDateStruct", {}).get("date")

        if not nct_id or not updated_date:
            raise ValueError("Study is missing its NCT ID or last update date.")

        updated_date = datetime.strptime(updated_date, "%Y-%m-%d").date()
        rows.append((nct_id, updated_date, Json(study), context["run_id"]))

    return load_rows("ctgov_studies", "nct_id", "source_updated_date", rows)


def ingest_faers(**context):
    params = context["params"]
    received_from = date.fromisoformat(params["faers_received_from"])
    received_to = date.fromisoformat(params["faers_received_to"])

    if received_from > received_to:
        raise ValueError("faers_received_from must not be after faers_received_to.")

    api_key = os.getenv("OPENFDA_API_KEY")
    if not api_key:
        raise ValueError("OPENFDA_API_KEY is not set.")

    response = requests.get(
        "https://api.fda.gov/drug/event.json",
        params={
            "search": f"receivedate:[{received_from:%Y%m%d} TO {received_to:%Y%m%d}]",
            "sort": "receivedate:asc",
            "limit": params["record_limit"],
        },
        auth=(api_key, ""),
        timeout=60,
    )
    response.raise_for_status()

    rows = []
    for report in response.json()["results"]:
        report_id = report.get("safetyreportid")
        report_version = report.get("safetyreportversion")

        if not report_id:
            raise ValueError("Report is missing its safetyreportid.")

        if not report_version:
            # No version supplied: derive a stable one from the content.
            report_text = json.dumps(report, sort_keys=True)
            report_version = "sha256:" + hashlib.sha256(report_text.encode("utf-8")).hexdigest()

        rows.append((str(report_id), str(report_version), Json(report), context["run_id"]))

    return load_rows("faers_reports", "report_id", "report_version", rows)


def check_tables(**context):
    """Data-quality gate: fetched records all landed in raw, with keys and payloads present."""
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    ti = context["ti"]
    problems = []

    sources = [
        ("ingest_ctgov", "ctgov_studies", "nct_id", "source_updated_date"),
        ("ingest_faers", "faers_reports", "report_id", "report_version"),
    ]

    for task_id, table, id_column, version_column in sources:
        keys = ti.xcom_pull(task_ids=task_id) or []
        ids = [key[0] for key in keys]
        versions = [key[1] for key in keys]

        # 1. Completeness: every record fetched in this run exists in raw.
        found = pg.get_first(
            f"""
            SELECT count(*)
            FROM raw.{table} AS t
            JOIN unnest(%s::text[], %s::text[]) AS k(id, version)
              ON t.{id_column} = k.id AND t.{version_column}::text = k.version
            """,
            parameters=(ids, versions),
        )[0]

        # 2. Nulls / empty payloads anywhere in the table.
        bad = pg.get_first(
            f"""
            SELECT count(*) FROM raw.{table}
            WHERE {id_column} IS NULL OR {version_column} IS NULL
               OR payload IS NULL OR payload = '{{}}'::jsonb
            """
        )[0]

        total = pg.get_first(f"SELECT count(DISTINCT {id_column}) FROM raw.{table}")[0]

        print(f"raw.{table}: fetched {len(keys)}, found in raw {found}, "
              f"bad rows {bad}, distinct IDs in table {total}")

        if not keys:
            problems.append(f"{task_id} returned no records")
        if found != len(keys):
            problems.append(f"raw.{table} is missing {len(keys) - found} fetched records")
        if bad:
            problems.append(f"raw.{table} has {bad} rows with null keys or empty payloads")

    if problems:
        raise ValueError("Data-quality check failed: " + "; ".join(problems))


with DAG(
    dag_id="ingest_sources",
    description="Ingest ClinicalTrials.gov + FAERS into raw, then build silver and gold with dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "ctgov_updated_since": Param(
            "2025-01-01", type="string", format="date",
            description="ClinicalTrials.gov: studies last updated on or after this date.",
        ),
        "faers_received_from": Param(
            "2025-01-01", type="string", format="date",
            description="FAERS: reports received by the FDA on or after this date.",
        ),
        "faers_received_to": Param(
            "2025-03-31", type="string", format="date",
            description="FAERS: reports received by the FDA on or before this date.",
        ),
        "record_limit": Param(
            MAX_PAGE_SIZE, type="integer", minimum=1, maximum=MAX_PAGE_SIZE,
            description="Maximum records fetched per source (one API page).",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
) as dag:

    ctgov = PythonOperator(task_id="ingest_ctgov", python_callable=ingest_ctgov)
    faers = PythonOperator(task_id="ingest_faers", python_callable=ingest_faers)
    checks = PythonOperator(task_id="check_tables", python_callable=check_tables)

    dbt_command = (
        "/opt/dbt/bin/dbt {command} --project-dir /opt/airflow/dbt "
        "--profiles-dir /opt/airflow/dbt --target-path /opt/airflow/dbt-target "
        "--log-path /opt/airflow/dbt-logs "
    )
    raw_tests = BashOperator(
        task_id="test_raw",
        bash_command=dbt_command.format(command="test") + "--select source:raw",
    )
    silver = BashOperator(
        task_id="build_silver",
        bash_command=dbt_command.format(command="build") + "--select path:models/silver --indirect-selection eager",
    )
    gold = BashOperator(
        task_id="build_gold",
        bash_command=dbt_command.format(command="build") + "--select path:models/gold snap_ct__studies --indirect-selection eager",
    )

    [ctgov, faers] >> checks >> raw_tests >> silver >> gold
