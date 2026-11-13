import hashlib
import json
import os
from airflow.models.param import Param
from contextlib import closing
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json


LIMIT = 1000


def load_rows(table, id_column, version_column, rows):
    if len(rows) != LIMIT:
        raise ValueError(f"Oodati {LIMIT} kirjet, saadi {len(rows)}.")

    ids = [row[0] for row in rows]

    if len(set(ids)) != LIMIT:
        raise ValueError("Sisendis on korduvaid kirje ID-sid.")

    sql = f"""
        INSERT INTO raw.{table} (
            {id_column},
            {version_column},
            payload,
            batch_id
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ({id_column}, {version_column})
        DO NOTHING
    """

    pg = PostgresHook(postgres_conn_id="women_db")
    inserted = 0

    with closing(pg.get_conn()) as connection:
        with connection:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(sql, row)
                    inserted += cursor.rowcount

    print(f"Sisendis: {len(rows)}")
    print(f"Lisatud: {inserted}")
    print(f"Kordusi: {len(rows) - inserted}")


def ingest_ctgov(**context):
    record_limit = context["params"]["record_limit"]
    query = (
        "AREA[StudyType]INTERVENTIONAL"
        " AND (AREA[InterventionType]DRUG"
        " OR AREA[InterventionType]BIOLOGICAL)"
    )

    response = requests.get(
        "https://clinicaltrials.gov/api/v2/studies",
        params={
            "filter.advanced": query,
            "pageSize": record_limit,
            "format": "json",
        },
        timeout=60,
    )
    response.raise_for_status()

    studies = response.json()["studies"]
    rows = []

    for study in studies:
        protocol = study.get("protocolSection", {})

        nct_id = protocol.get(
            "identificationModule", {}
        ).get("nctId")

        updated_date = protocol.get(
            "statusModule", {}
        ).get("lastUpdatePostDateStruct", {}).get("date")

        if not nct_id or not updated_date:
            raise ValueError("Uuringul puudub ID või uuendamiskuupäev.")

        updated_date = datetime.strptime(
            updated_date, "%Y-%m-%d"
        ).date()

        rows.append((
            nct_id,
            updated_date,
            Json(study),
            context["run_id"],
        ))

    load_rows(
        "ctgov_studies",
        "nct_id",
        "source_updated_date",
        rows,
    )


def ingest_faers(**context):
    record_limit = context["params"]["record_limit"]
    api_key = os.getenv("OPENFDA_API_KEY")

    if not api_key:
        raise ValueError("OPENFDA_API_KEY puudub.")

    response = requests.get(
        "https://api.fda.gov/drug/event.json",
        params={"limit": record_limit},
        auth=(api_key, ""),
        timeout=60,
    )
    response.raise_for_status()

    reports = response.json()["results"]
    rows = []

    for report in reports:
        report_id = report.get("safetyreportid")
        report_version = report.get("safetyreportversion")

        if not report_id:
            raise ValueError("Teatel puudub ID.")

        if not report_version:
            report_text = json.dumps(report, sort_keys=True)
            report_version = "sha256:" + hashlib.sha256(
                report_text.encode("utf-8")
            ).hexdigest()

        rows.append((
            str(report_id),
            str(report_version),
            Json(report),
            context["run_id"],
        ))

    load_rows(
        "faers_reports",
        "report_id",
        "report_version",
        rows,
    )


def check_tables():
    pg = PostgresHook(postgres_conn_id="women_db")

    studies = pg.get_first(
        "SELECT COUNT(DISTINCT nct_id) FROM raw.ctgov_studies"
    )[0]

    reports = pg.get_first(
        "SELECT COUNT(DISTINCT report_id) FROM raw.faers_reports"
    )[0]

    print(f"Uuringuid: {studies}")
    print(f"FAERS teateid: {reports}")

    if studies < LIMIT:
        raise ValueError("Uuringuid on vähem kui 1000.")

    if reports < LIMIT:
        raise ValueError("FAERS teateid on vähem kui 1000.")


with DAG(
    dag_id="ingest_sources",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "record_limit": Param(
            1000,
            type="integer",
            minimum=1000,
            maximum=1000,
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
) as dag:

    ctgov = PythonOperator(
        task_id="ingest_ctgov",
        python_callable=ingest_ctgov,
    )

    faers = PythonOperator(
        task_id="ingest_faers",
        python_callable=ingest_faers,
    )

    checks = PythonOperator(
        task_id="check_tables",
        python_callable=check_tables,
    )

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
