
print("Skript käivitus", flush=True)
import hashlib
import json
import os
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb


# Projekti põhikaust ja ühenduse seadistused
PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")


def main():
    input_path = PROJECT_DIR / "sample_data" / "ctg_studies_raw.json"

    # Loe olemasolev väljavõte; uut API-päringut ei tehta
    file_bytes = input_path.read_bytes()
    studies = json.loads(file_bytes)

    if not isinstance(studies, list) or not studies:
        raise ValueError("Sisend peab olema mittetühi uuringute loend.")

    # Sama fail saab korduskäivitusel sama väljavõtte tunnuse
    batch_id = hashlib.sha256(file_bytes).hexdigest()

    rows = []

    # Kontrolli kõik kirjed enne laadimist
    for study in studies:
        protocol = study.get("protocolSection", {})

        nct_id = protocol.get(
            "identificationModule", {}
        ).get("nctId")

        updated_date = protocol.get(
            "statusModule", {}
        ).get("lastUpdatePostDateStruct", {}).get("date")

        if not nct_id or not updated_date:
            raise ValueError(
                f"Uuringul puudub ID või uuendamiskuupäev: {nct_id}"
            )

        rows.append((
            nct_id,
            date.fromisoformat(updated_date),
            Jsonb(study),
            batch_id,
        ))

    # Python töötab sinu arvutis, seega localhost ja väline port
    with psycopg.connect(
        connect_timeout=10,
        host="127.0.0.1",
        port=int(os.environ["RESEARCH_DB_PORT"]),
        dbname=os.environ["RESEARCH_DB_NAME"],
        user=os.environ["RESEARCH_DB_USER"],
        password=os.environ["RESEARCH_DB_PASSWORD"],
    ) as connection:
        with connection.cursor() as cursor:
            inserted = 0

            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO raw.ctgov_studies (
                        nct_id,
                        source_updated_date,
                        payload,
                        batch_id
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (nct_id, source_updated_date)
                    DO NOTHING
                    RETURNING nct_id
                    """,
                    row,
                )

                if cursor.fetchone() is not None:
                    inserted += 1

    print(f"Failis oli {len(studies)} uuringut.")
    print(f"Lisati {inserted} uut uuringu versiooni.")
    print(f"Olemasolevaid kordusi oli {len(studies) - inserted}.")


if __name__ == "__main__":
    main()