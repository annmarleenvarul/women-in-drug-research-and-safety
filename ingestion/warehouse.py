"""Loading raw API records into the bronze layer.

This module is the storage-side half of the ingestion contract (project section 2.2).
The Airflow tasks (2.1) are responsible for calling the APIs; they hand the records to
`load_bronze`, which owns everything about how they are stored.

Idempotency
-----------
`load_bronze` runs DELETE + INSERT for one (source, load_date) inside a single
transaction. Re-running a DAG for the same logical date therefore replaces exactly that
date's rows instead of duplicating them, and a crash mid-load leaves the previous
contents untouched because the transaction is rolled back.
"""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import date
from typing import Any, Callable, Iterable, Iterator, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

from .config import WarehouseConfig

log = logging.getLogger(__name__)

# Which column holds the natural key of each bronze table, and how to read it
# out of one API record. Adding a third source means adding one entry here.
SOURCES: dict[str, dict[str, Any]] = {
    "faers": {
        "table": "bronze.faers_reports",
        "key_column": "safetyreportid",
        "key_getter": lambda rec: rec.get("safetyreportid"),
    },
    "clinicaltrials": {
        "table": "bronze.ct_studies",
        "key_column": "nct_id",
        "key_getter": lambda rec: (
            rec.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
        ),
    },
}


@contextmanager
def warehouse_connection(config: WarehouseConfig | None = None) -> Iterator[Any]:
    """Open a connection, commit on success, roll back on any exception."""
    config = config or WarehouseConfig.from_env()
    conn = psycopg2.connect(config.dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _chunks(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def load_bronze(
    source_name: str,
    records: Iterable[dict],
    load_date: date,
    *,
    config: WarehouseConfig | None = None,
    dag_run_id: str | None = None,
    batch_size: int = 1_000,
) -> dict[str, int]:
    """Replace one load_date's rows of one source with `records`.

    Returns {"deleted": n, "inserted": n}. Records whose natural key cannot be read are
    skipped and counted in the log, because a key-less row could never be joined later.
    """
    if source_name not in SOURCES:
        raise ValueError(
            f"Unknown source {source_name!r}; expected one of {sorted(SOURCES)}"
        )

    spec = SOURCES[source_name]
    table: str = spec["table"]
    key_column: str = spec["key_column"]
    key_getter: Callable[[dict], str | None] = spec["key_getter"]

    rows, skipped = [], 0
    for record in records:
        key = key_getter(record)
        if key is None:
            skipped += 1
            continue
        rows.append((str(key), Json(record), load_date))

    config = config or WarehouseConfig.from_env()
    with warehouse_connection(config) as conn, conn.cursor() as cur:
        # One transaction: the old rows for this date disappear only if the new ones land.
        cur.execute(f"delete from {table} where load_date = %s", (load_date,))
        deleted = cur.rowcount

        inserted = 0
        for batch in _chunks(rows, batch_size):
            execute_values(
                cur,
                f"insert into {table} ({key_column}, payload, load_date) values %s",
                batch,
            )
            inserted += len(batch)

        message = f"{skipped} record(s) without a natural key were skipped" if skipped else None
        cur.execute(
            """
            insert into bronze.ingestion_log
                (source_name, load_date, dag_run_id, rows_deleted, rows_inserted, status, message)
            values (%s, %s, %s, %s, %s, 'success', %s)
            """,
            (source_name, load_date, dag_run_id, deleted, inserted, message),
        )

    log.info(
        "bronze load %s %s: deleted %s, inserted %s, skipped %s",
        source_name, load_date, deleted, inserted, skipped,
    )
    return {"deleted": deleted, "inserted": inserted, "skipped": skipped}


def log_failure(
    source_name: str,
    load_date: date,
    message: str,
    *,
    config: WarehouseConfig | None = None,
    dag_run_id: str | None = None,
) -> None:
    """Record a failed load so the audit log shows attempts, not just successes."""
    with warehouse_connection(config) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into bronze.ingestion_log
                (source_name, load_date, dag_run_id, status, message)
            values (%s, %s, %s, 'failed', %s)
            """,
            (source_name, load_date, dag_run_id, message[:2000]),
        )


def bronze_row_count(
    source_name: str, load_date: date | None = None, *, config: WarehouseConfig | None = None
) -> int:
    """Row count for a source, optionally for one load_date. Used by data-quality checks."""
    table = SOURCES[source_name]["table"]
    sql = f"select count(*) from {table}"
    params: tuple = ()
    if load_date is not None:
        sql += " where load_date = %s"
        params = (load_date,)
    with warehouse_connection(config) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]
