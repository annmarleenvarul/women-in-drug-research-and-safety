#!/usr/bin/env python3
"""Create the medallion DuckDB warehouse"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "warehouse" / "women_drug_safety.duckdb"
SQL_FILE = ROOT / "sql" / "medallion_schema.sql"


def parse_int(value):
    if value is None or value == "":
        return None
    return int(value)


def ensure_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(SQL_FILE.read_text(encoding="utf-8"))
    return conn


def read_csv_records(path: Path):
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_raw_csvs(conn: duckdb.DuckDBPyConnection) -> None:
    fda_rows = read_csv_records(ROOT / "sample_data" / "fda_reports_sample.csv")
    cts_rows = read_csv_records(ROOT / "sample_data" / "ctg_studies_sample.csv")

    fda_columns = [
        "safetyreportid",
        "receivedate",
        "serious",
        "seriousnessdeath",
        "seriousnesshospitalization",
        "seriousnesslifethreatening",
        "patientonsetage",
        "patientonsetageunit",
        "patientsex",
        "reactionmeddrapt",
        "medicinalproduct",
        "drugcharacterization",
    ]
    cts_columns = [
        "NCT Number",
        "Study Title",
        "Study Status",
        "Conditions",
        "Interventions",
        "Sex",
        "Age",
        "Phases",
        "Enrollment",
        "Funder Type",
        "Study Type",
        "Start Date",
        "Completion Date",
        "Locations",
    ]

    conn.execute(
        "CREATE OR REPLACE TABLE bronze.fda_reports_raw ("
        "safetyreportid VARCHAR, receivedate VARCHAR, serious INTEGER, seriousnessdeath INTEGER, "
        "seriousnesshospitalization INTEGER, seriousnesslifethreatening INTEGER, patientonsetage DOUBLE, "
        "patientonsetageunit INTEGER, patientsex INTEGER, reactionmeddrapt VARCHAR, medicinalproduct VARCHAR, "
        "drugcharacterization VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO bronze.fda_reports_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                row.get("safetyreportid", ""),
                row.get("receivedate", ""),
                parse_int(row.get("serious")),
                parse_int(row.get("seriousnessdeath")),
                parse_int(row.get("seriousnesshospitalization")),
                parse_int(row.get("seriousnesslifethreatening")),
                parse_int(row.get("patientonsetage")) if row.get("patientonsetage") not in (None, "") else None,
                parse_int(row.get("patientonsetageunit")),
                parse_int(row.get("patientsex")),
                row.get("reactionmeddrapt", ""),
                row.get("medicinalproduct", ""),
                row.get("drugcharacterization", ""),
            )
            for row in fda_rows
        ],
    )

    conn.execute(
        "CREATE OR REPLACE TABLE bronze.ctg_studies_raw ("
        "nct_id VARCHAR, study_title VARCHAR, study_status VARCHAR, conditions VARCHAR, interventions VARCHAR, "
        "sex VARCHAR, age VARCHAR, phases VARCHAR, enrollment INTEGER, funder_type VARCHAR, study_type VARCHAR, "
        "start_date VARCHAR, completion_date VARCHAR, locations VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO bronze.ctg_studies_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                row.get("NCT Number", ""),
                row.get("Study Title", ""),
                row.get("Study Status", ""),
                row.get("Conditions", ""),
                row.get("Interventions", ""),
                row.get("Sex", ""),
                row.get("Age", ""),
                row.get("Phases", ""),
                parse_int(row.get("Enrollment")),
                row.get("Funder Type", ""),
                row.get("Study Type", ""),
                row.get("Start Date", ""),
                row.get("Completion Date", ""),
                row.get("Locations", ""),
            )
            for row in cts_rows
        ],
    )


def build_dim_date(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE OR REPLACE TABLE gold.dim_date (date_key INTEGER PRIMARY KEY, full_date DATE, year SMALLINT, quarter SMALLINT, month SMALLINT)"
    )
    start = date(2000, 1, 1)
    end = date(2035, 12, 31)
    rows = []
    current = start
    while current <= end:
        rows.append(
            (
                int(current.strftime("%Y%m%d")),
                current,
                current.year,
                ((current.month - 1) // 3) + 1,
                current.month,
            )
        )
        current += timedelta(days=1)
    conn.executemany(
        "INSERT INTO gold.dim_date VALUES (?, ?, ?, ?, ?)",
        rows,
    )


def build_dimensions(conn: duckdb.DuckDBPyConnection) -> None:
    reactions = sorted(
        {
            token.strip()
            for row in read_csv_records(ROOT / "sample_data" / "fda_reports_sample.csv")
            for token in (row.get("reactionmeddrapt") or "").split("|")
            if token.strip()
        }
    )
    conn.execute("CREATE OR REPLACE TABLE gold.dim_reaction (reaction_key INTEGER PRIMARY KEY, meddra_pt VARCHAR)")
    conn.executemany(
        "INSERT INTO gold.dim_reaction VALUES (?, ?)",
        [(idx + 1, name) for idx, name in enumerate(reactions)],
    )

    patient_profiles = []
    seen = set()
    for row in read_csv_records(ROOT / "sample_data" / "fda_reports_sample.csv"):
        patientsex = row.get("patientsex")
        age_unit = row.get("patientonsetageunit")
        sex = "female" if patientsex == "1" else "male" if patientsex == "2" else "unknown"
        age_band = "unknown"
        if age_unit == "801":
            age_band = "18-64"
        elif age_unit == "802":
            age_band = "0-17"
        elif age_unit == "803":
            age_band = "65+"
        profile = (sex, age_band)
        if profile not in seen:
            seen.add(profile)
            patient_profiles.append(profile)
    conn.execute("CREATE OR REPLACE TABLE gold.dim_patient_profile (patient_profile_key INTEGER PRIMARY KEY, sex VARCHAR, age_band VARCHAR)")
    conn.executemany(
        "INSERT INTO gold.dim_patient_profile VALUES (?, ?, ?)",
        [(idx + 1, sex, age_band) for idx, (sex, age_band) in enumerate(patient_profiles)],
    )

    products = sorted(
        {
            token.strip()
            for row in read_csv_records(ROOT / "sample_data" / "fda_reports_sample.csv")
            for token in (row.get("medicinalproduct") or "").split("|")
            if token.strip()
        }
    )
    conn.execute("CREATE OR REPLACE TABLE gold.dim_drug (drug_key INTEGER PRIMARY KEY, rxcui VARCHAR, ingredient_name VARCHAR, drug_class VARCHAR)")
    conn.executemany(
        "INSERT INTO gold.dim_drug VALUES (?, ?, ?, ?)",
        [(idx + 1, None, name, None) for idx, name in enumerate(products)],
    )

    trials = read_csv_records(ROOT / "sample_data" / "ctg_studies_sample.csv")
    conn.execute("CREATE OR REPLACE TABLE gold.dim_trial (trial_key INTEGER PRIMARY KEY, nct_id VARCHAR, title VARCHAR, overall_status VARCHAR, phase VARCHAR, eligible_sex VARCHAR, min_age_years NUMERIC(5,1), max_age_years NUMERIC(5,1), valid_from DATE, valid_to DATE, is_current BOOLEAN)")
    conn.executemany(
        "INSERT INTO gold.dim_trial VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                idx + 1,
                row.get("NCT Number", ""),
                row.get("Study Title", ""),
                row.get("Study Status", ""),
                row.get("Phases", ""),
                row.get("Sex", ""),
                None,
                None,
                None,
                None,
                True,
            )
            for idx, row in enumerate(trials)
        ],
    )

    conditions = sorted(
        {
            token.strip()
            for row in read_csv_records(ROOT / "sample_data" / "ctg_studies_sample.csv")
            for token in (row.get("Conditions") or "").split("|")
            if token.strip()
        }
    )
    conn.execute("CREATE OR REPLACE TABLE gold.dim_condition (condition_key INTEGER PRIMARY KEY, condition_name VARCHAR, condition_class VARCHAR)")
    conn.executemany(
        "INSERT INTO gold.dim_condition VALUES (?, ?, ?)",
        [(idx + 1, name, None) for idx, name in enumerate(conditions)],
    )

    conn.execute("CREATE OR REPLACE TABLE gold.bridge_trial_condition (nct_id VARCHAR, condition_key INTEGER, PRIMARY KEY (nct_id, condition_key))")
    rows = []
    for row in trials:
        nct = row.get("NCT Number", "")
        for condition_name in [token.strip() for token in (row.get("Conditions") or "").split("|") if token.strip()]:
            cond_key = next(
                idx + 1
                for idx, name in enumerate(conditions)
                if name == condition_name
            )
            rows.append((nct, cond_key))
    conn.executemany("INSERT INTO gold.bridge_trial_condition VALUES (?, ?)", rows)


def build_fact_tables(conn: duckdb.DuckDBPyConnection) -> None:
    fda_rows = read_csv_records(ROOT / "sample_data" / "fda_reports_sample.csv")
    trial_rows = read_csv_records(ROOT / "sample_data" / "ctg_studies_sample.csv")

    reaction_map = {
        name: idx + 1
        for idx, name in enumerate(
            sorted(
                {
                    token.strip()
                    for row in fda_rows
                    for token in (row.get("reactionmeddrapt") or "").split("|")
                    if token.strip()
                }
            )
        )
    }
    drug_map = {
        name: idx + 1
        for idx, name in enumerate(
            sorted(
                {
                    token.strip()
                    for row in fda_rows
                    for token in (row.get("medicinalproduct") or "").split("|")
                    if token.strip()
                }
            )
        )
    }
    patient_map = {}
    for row in fda_rows:
        sex = "female" if row.get("patientsex") == "1" else "male" if row.get("patientsex") == "2" else "unknown"
        age_unit = row.get("patientonsetageunit")
        if age_unit == "801":
            age_band = "18-64"
        elif age_unit == "802":
            age_band = "0-17"
        elif age_unit == "803":
            age_band = "65+"
        else:
            age_band = "unknown"
        profile = (sex, age_band)
        if profile not in patient_map:
            patient_map[profile] = len(patient_map) + 1

    conn.execute("CREATE OR REPLACE TABLE gold.fact_adverse_event (ae_key BIGINT PRIMARY KEY, report_id VARCHAR, received_date_key INTEGER, drug_key INTEGER, reaction_key INTEGER, patient_profile_key INTEGER, is_serious BOOLEAN, is_death BOOLEAN, is_hospitalisation BOOLEAN, is_life_threatening BOOLEAN)")
    fact_rows = []
    for row in fda_rows:
        received = row.get("receivedate", "")
        if len(received) == 8:
            date_key = int(received)
        else:
            date_key = None
        for drug_name in [token.strip() for token in (row.get("medicinalproduct") or "").split("|") if token.strip()]:
            for reaction_name in [token.strip() for token in (row.get("reactionmeddrapt") or "").split("|") if token.strip()]:
                sex = "female" if row.get("patientsex") == "1" else "male" if row.get("patientsex") == "2" else "unknown"
                age_unit = row.get("patientonsetageunit")
                if age_unit == "801":
                    age_band = "18-64"
                elif age_unit == "802":
                    age_band = "0-17"
                elif age_unit == "803":
                    age_band = "65+"
                else:
                    age_band = "unknown"
                fact_rows.append(
                    (
                        len(fact_rows) + 1,
                        row.get("safetyreportid", ""),
                        date_key,
                        drug_map.get(drug_name),
                        reaction_map.get(reaction_name),
                        patient_map.get((sex, age_band)),
                        row.get("serious") == "1",
                        row.get("seriousnessdeath") == "1",
                        row.get("seriousnesshospitalization") == "1",
                        row.get("seriousnesslifethreatening") == "1",
                    )
                )
    conn.executemany("INSERT INTO gold.fact_adverse_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", fact_rows)

    trial_map = {row.get("NCT Number", ""): idx + 1 for idx, row in enumerate(trial_rows)}
    drug_lookup = {name: idx + 1 for idx, name in enumerate(sorted({token.strip() for row in fda_rows for token in (row.get("medicinalproduct") or "").split("|") if token.strip()}))}
    conn.execute("CREATE OR REPLACE TABLE gold.fact_trial_drug (trial_drug_key BIGINT PRIMARY KEY, trial_key INTEGER, drug_key INTEGER, start_date_key INTEGER, enrollment INTEGER, is_enrollment_actual BOOLEAN)")
    trial_fact_rows = []
    for row in trial_rows:
        nct = row.get("NCT Number", "")
        interventions = [token.strip() for token in (row.get("Interventions") or "").split("|") if token.strip()]
        for intervention in interventions:
            item = intervention.replace("DRUG: ", "").replace("BIOLOGICAL: ", "").strip()
            if item and item.lower() in {name.lower() for name in drug_lookup.keys()}:
                start_raw = row.get("Start Date", "")
                if len(start_raw) >= 4:
                    try:
                        start_key = int(start_raw[:4] + start_raw[5:7].zfill(2) + start_raw[8:10].zfill(2))
                    except Exception:
                        start_key = None
                else:
                    start_key = None
                trial_fact_rows.append(
                    (
                        len(trial_fact_rows) + 1,
                        trial_map.get(nct),
                        drug_lookup.get(next(name for name in drug_lookup if name.lower() == item.lower())),
                        start_key,
                        int(row.get("Enrollment", 0) or 0),
                        True,
                    )
                )
    if trial_fact_rows:
        conn.executemany("INSERT INTO gold.fact_trial_drug VALUES (?, ?, ?, ?, ?, ?)", trial_fact_rows)


def main() -> None:
    conn = ensure_db()
    load_raw_csvs(conn)
    build_dim_date(conn)
    build_dimensions(conn)
    build_fact_tables(conn)

    print(f"DuckDB warehouse ready at: {DB_PATH}")
    print("Bronze rows:", conn.execute("SELECT COUNT(*) FROM bronze.fda_reports_raw").fetchone()[0])
    print("Reaction dimensions:", conn.execute("SELECT COUNT(*) FROM gold.dim_reaction").fetchone()[0])
    print("Fact adverse events:", conn.execute("SELECT COUNT(*) FROM gold.fact_adverse_event").fetchone()[0])
    print("Fact trial drugs:", conn.execute("SELECT COUNT(*) FROM gold.fact_trial_drug").fetchone()[0])
    conn.close()


if __name__ == "__main__":
    main()
