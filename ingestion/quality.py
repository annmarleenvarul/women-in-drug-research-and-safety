"""Data-quality checks run against the bronze layer (project section 2.1).

Kept next to the loader so the checks use the same connection handling. An Airflow task
calls `run_bronze_checks(...)`; raising stops the DAG before dbt runs, which keeps the
previous gold tables live.
"""
from __future__ import annotations

import logging
from datetime import date

from .config import WarehouseConfig
from .warehouse import SOURCES, warehouse_connection

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a bronze data-quality check fails."""


def run_bronze_checks(
    source_name: str,
    load_date: date,
    *,
    config: WarehouseConfig | None = None,
    min_rows: int = 1,
    max_null_key_fraction: float = 0.0,
    max_duplicate_key_fraction: float = 0.5,
) -> dict[str, float]:
    """Check one source's rows for a load_date.

    Checks
      1. row count              -- at least `min_rows`, so an empty API response is caught
      2. missing natural keys   -- a row that cannot be joined later
      3. duplicate natural keys -- expected in small numbers (follow-up report versions),
                                   but a high share means the pagination repeated itself
    Returns the measured metrics; raises DataQualityError on the first failure.
    """
    spec = SOURCES[source_name]
    table, key_column = spec["table"], spec["key_column"]

    with warehouse_connection(config) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            select count(*),
                   count(*) filter (where {key_column} is null or {key_column} = ''),
                   count(*) - count(distinct {key_column})
            from {table}
            where load_date = %s
            """,
            (load_date,),
        )
        total, null_keys, duplicate_keys = cur.fetchone()

    metrics = {
        "rows": total,
        "null_keys": null_keys,
        "duplicate_keys": duplicate_keys,
        "null_key_fraction": (null_keys / total) if total else 0.0,
        "duplicate_key_fraction": (duplicate_keys / total) if total else 0.0,
    }
    log.info("bronze quality %s %s: %s", source_name, load_date, metrics)

    if total < min_rows:
        raise DataQualityError(
            f"{source_name} {load_date}: loaded {total} row(s), expected at least {min_rows}"
        )
    if metrics["null_key_fraction"] > max_null_key_fraction:
        raise DataQualityError(
            f"{source_name} {load_date}: {null_keys} row(s) without a natural key"
        )
    if metrics["duplicate_key_fraction"] > max_duplicate_key_fraction:
        raise DataQualityError(
            f"{source_name} {load_date}: {duplicate_keys} duplicate key(s) "
            f"({metrics['duplicate_key_fraction']:.0%} of rows) — check API pagination"
        )
    return metrics
