"""Late arrival detection logic for the late_arrival_sensor DAG.

Queries the bronze partition manifest against current Iceberg $partitions
metadata to find dates where new rows arrived after the batch job published.
Triggers batch_backfill for any date exceeding LATE_ARRIVAL_THRESHOLD.

Kept separate from airflow_batch_tasks.py — this is sensor/detection concern,
not part of the daily batch pipeline.
"""

from __future__ import annotations

import os

from datetime import datetime, timezone

from .airflow_batch_tasks import (
    LATE_ARRIVAL_THRESHOLD,
    MANIFEST_TABLE,
    _trino_connection,
)

TRIGGER_LOG_TABLE = "lakehouse.qa.late_arrival_trigger_log"


def _ensure_trigger_log_table(cursor) -> None:
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TRIGGER_LOG_TABLE} (
            event_date   DATE,
            triggered_at TIMESTAMP(6)
        )
        WITH (
            partitioning = ARRAY['days(event_date)']
        )
    """)


def _write_trigger_log(cursor, event_date: str) -> None:
    triggered_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    cursor.execute(f"""
        INSERT INTO {TRIGGER_LOG_TABLE} (event_date, triggered_at)
        VALUES (DATE '{event_date}', TIMESTAMP '{triggered_at}')
    """)


def detect_and_trigger_late_arrival_backfill(*, trigger_dag_run_fn) -> int:
    """Query manifest vs bronze $partitions; trigger backfill for dates above threshold.

    Returns the number of dates for which a backfill was triggered.

    Both sides of the comparison read from Iceberg $partitions metadata —
    O(partitions), not O(rows).

    Idempotent: skips dates that already have a trigger log entry newer than
    the latest manifest completed_at. The suppression clears automatically
    once batch_publish_daily writes a new manifest row after backfill completes.

    trigger_dag_run_fn(event_date: str) is injected by the DAG to avoid
    coupling this helper to Airflow imports.
    """
    conn = _trino_connection()
    try:
        cur = conn.cursor()
        _ensure_trigger_log_table(cur)
        cur.execute(f"""
            SELECT m.event_date
            FROM (
                SELECT event_date,
                       MAX(bronze_row_count) AS baseline,
                       MAX(completed_at)     AS last_completed_at
                FROM {MANIFEST_TABLE}
                GROUP BY event_date
            ) m
            JOIN (
                SELECT partition.event_date AS event_date, SUM(record_count) AS current_count
                FROM "lakehouse"."bronze"."raw_events$partitions"
                GROUP BY partition.event_date
            ) b ON b.event_date = m.event_date
            LEFT JOIN (
                SELECT event_date, MAX(triggered_at) AS last_triggered_at
                FROM {TRIGGER_LOG_TABLE}
                GROUP BY event_date
            ) t ON t.event_date = m.event_date
            WHERE b.current_count - m.baseline > {LATE_ARRIVAL_THRESHOLD}
              AND (t.last_triggered_at IS NULL OR t.last_triggered_at < m.last_completed_at)
        """)
        rows = cur.fetchall()

        triggered = 0
        for (event_date,) in rows:
            date_str = str(event_date)
            _write_trigger_log(cur, date_str)
            print(
                f"[LATE-ARRIVAL-SENSOR] late arrivals detected for "
                f"event_date={date_str} — triggering backfill"
            )
            trigger_dag_run_fn(date_str)
            triggered += 1
    finally:
        conn.close()

    if triggered == 0:
        print(f"[LATE-ARRIVAL-SENSOR] no dates above threshold={LATE_ARRIVAL_THRESHOLD}")
    return triggered
