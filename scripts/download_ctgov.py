"""
Download interventional drug and biological trials from the ClinicalTrials.gov API v2.

Writes four files to sample_data/:
  ctg_studies_raw.json          studies exactly as returned by the API (raw landing)
  ctg_trials_sample.csv         one row per trial            -> dim_trial
  ctg_trial_drugs_sample.csv    one row per drug per trial   -> fact_trial_drug
  ctg_trial_conditions_sample.csv  one row per condition per trial
                                                            -> dim_condition, bridge_trial_condition

Usage:
  python scripts/download_ctgov.py --updated-since 2024-10-01 --max-studies 2000

--updated-since keeps only trials changed on or after that date. The daily Airflow
run in Project 2 uses the same filter with the date of the previous run, so each
run only fetches trials that changed. No API key is needed.
"""

import argparse
import csv
import json
import re
import time
from pathlib import Path

import requests

URL = "https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE = 1000          # API maximum per page
OUT_DIR = Path(__file__).resolve().parent.parent / "sample_data"
DRUG_TYPES = {"DRUG", "BIOLOGICAL"}

AGE_UNITS_IN_YEARS = {"year": 1, "month": 1 / 12, "week": 1 / 52.18, "day": 1 / 365.25,
                      "hour": 1 / 8766, "minute": 1 / 525960}


def fetch_studies(updated_since: str, max_studies: int) -> list[dict]:
    """Page through interventional drug/biological studies updated since a date."""
    query = (
        "AREA[StudyType]INTERVENTIONAL"
        " AND (AREA[InterventionType]DRUG OR AREA[InterventionType]BIOLOGICAL)"
        f" AND AREA[LastUpdatePostDate]RANGE[{updated_since},MAX]"
    )
    studies = []
    token = None
    while len(studies) < max_studies:
        params = {"filter.advanced": query, "pageSize": min(PAGE_SIZE, max_studies - len(studies)),
                  "format": "json"}
        if token:
            params["pageToken"] = token

        response = requests.get(URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        studies.extend(data.get("studies", []))
        print(f"Downloaded {len(studies)} studies")
        token = data.get("nextPageToken")
        if not token:
            break
        time.sleep(0.3)
    return studies


def age_in_years(text: str | None) -> float | None:
    """'18 Years' -> 18.0, '6 Months' -> 0.5, missing -> None (no limit)."""
    if not text:
        return None
    match = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    if not match:
        return None
    unit = match.group(2).lower().rstrip("s")
    factor = AGE_UNITS_IN_YEARS.get(unit)
    return round(float(match.group(1)) * factor, 1) if factor else None


def flatten(study: dict) -> tuple[dict, list[dict], list[dict]]:
    """Split one study into a trial row, its drug rows and its condition rows."""
    p = study.get("protocolSection", {})
    ident = p.get("identificationModule", {})
    status = p.get("statusModule", {})
    design = p.get("designModule", {})
    elig = p.get("eligibilityModule", {})
    enrollment = design.get("enrollmentInfo", {})
    nct_id = ident.get("nctId")

    trial = {
        "nct_id": nct_id,
        "title": ident.get("briefTitle"),
        "overall_status": status.get("overallStatus"),
        "phase": "/".join(design.get("phases", [])) or None,          # e.g. PHASE2/PHASE3
        "eligible_sex": elig.get("sex"),                               # ALL, FEMALE, MALE
        "minimum_age": elig.get("minimumAge"),
        "maximum_age": elig.get("maximumAge"),
        "min_age_years": age_in_years(elig.get("minimumAge")),
        "max_age_years": age_in_years(elig.get("maximumAge")),
        "start_date": status.get("startDateStruct", {}).get("date"),  # YYYY-MM or YYYY-MM-DD
        "enrollment": enrollment.get("count"),
        "enrollment_type": enrollment.get("type"),                     # ACTUAL or ESTIMATED
        "last_update_date": status.get("lastUpdatePostDateStruct", {}).get("date"),
    }

    interventions = p.get("armsInterventionsModule", {}).get("interventions", [])
    drugs = [{"nct_id": nct_id, "intervention_type": i.get("type"), "intervention_name": i.get("name")}
             for i in interventions if i.get("type") in DRUG_TYPES]

    # Conditions are free text; MeSH terms come as a separate list from the
    # registry's own matching. Attach a MeSH term to a condition only when the
    # match is unambiguous; otherwise condition_class stays empty and Q2 falls
    # back to condition_name.
    names = p.get("conditionsModule", {}).get("conditions", [])
    meshes = [m.get("term") for m in study.get("derivedSection", {})
              .get("conditionBrowseModule", {}).get("meshes", []) if m.get("term")]
    mesh_by_lower = {m.lower(): m for m in meshes}
    conditions = []
    for name in names:
        mesh = mesh_by_lower.get(name.lower())
        if mesh is None and len(names) == 1 and len(meshes) == 1:
            mesh = meshes[0]
        conditions.append({"nct_id": nct_id, "condition_name": name, "condition_class": mesh})

    return trial, drugs, conditions


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--updated-since", default="2024-10-01", help="YYYY-MM-DD (default 2024-10-01)")
    parser.add_argument("--max-studies", type=int, default=2000, help="maximum studies to download (default 2000)")
    args = parser.parse_args()

    studies = fetch_studies(args.updated_since, args.max_studies)

    OUT_DIR.mkdir(exist_ok=True)
    with open(OUT_DIR / "ctg_studies_raw.json", "w", encoding="utf-8") as f:
        json.dump(studies, f, ensure_ascii=False)

    trials, drugs, conditions = [], [], []
    for study in studies:
        t, d, c = flatten(study)
        trials.append(t)
        drugs.extend(d)
        conditions.extend(c)

    write_csv(OUT_DIR / "ctg_trials_sample.csv", trials)
    write_csv(OUT_DIR / "ctg_trial_drugs_sample.csv", drugs)
    write_csv(OUT_DIR / "ctg_trial_conditions_sample.csv", conditions)
    print(f"Done: {len(trials)} trials, {len(drugs)} trial-drug rows, {len(conditions)} trial-condition rows")


if __name__ == "__main__":
    main()
