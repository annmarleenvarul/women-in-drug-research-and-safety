import hashlib
import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb


PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")


def main():
    input_path = PROJECT_DIR / "sample_data" / "fda_reports_raw.json"
    file_bytes = input_path.read_bytes()
    reports = json.loads(file_bytes)

    if not isinstance(reports, list) or not reports:
        raise ValueError("Failis puudub teadete loend.")

    batch_id = hashlib.sha256(file_bytes).hexdigest()
    rows = []

    for report in reports:
        report_id = report.get("safetyreportid")
        report_version = report.get("safetyreportversion")

        if not report_id:
            raise ValueError("Teatel puudub safetyreportid.")

        if not report_version:
            report_text = json.dumps(report, sort_keys=True)
            report_version = "sha256:" + hashlib.sha256(
                report_text.encode("utf-8")
            ).hexdigest()

        rows.append((
            str(report_id),
            str(report_version),
            Jsonb(report),
            batch_id,
        ))

    with psycopg.connect(
        host="127.0.0.1",
        port=int(os.environ["RESEARCH_DB_PORT"]),
        dbname=os.environ["RESEARCH_DB_NAME"],
        user=os.environ["RESEARCH_DB_USER"],
        password=os.environ["RESEARCH_DB_PASSWORD"],
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            inserted = 0

            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO raw.faers_reports (
                        report_id,
                        report_version,
                        payload,
                        batch_id
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (report_id, report_version)
                    DO NOTHING
                    RETURNING report_id
                    """,
                    row,
                )

                if cursor.fetchone() is not None:
                    inserted += 1

    print(f"Failis oli {len(reports)} teadet.")
    print(f"Lisati {inserted} uut teate versiooni.")
    print(f"Kordusi oli {len(reports) - inserted}.")


if __name__ == "__main__":
    main()