import argparse
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    if not 1 <= args.limit <= 1000:
        raise ValueError("Limit peab olema vahemikus 1–1000.")

    params = {"limit": args.limit}

    api_key = os.getenv("OPENFDA_API_KEY")
    if api_key:
        params["api_key"] = api_key

    response = requests.get(
        "https://api.fda.gov/drug/event.json",
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    reports = response.json()["results"]

    output_dir = PROJECT_DIR / "sample_data"
    output_dir.mkdir(exist_ok=True)

    with open(
        output_dir / "fda_reports_raw.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(reports, file, ensure_ascii=False, indent=2)

    print(f"Salvestati {len(reports)} FAERS teadet.")


if __name__ == "__main__":
    main()