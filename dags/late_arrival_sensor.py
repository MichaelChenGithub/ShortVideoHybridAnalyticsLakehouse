"""Airflow DAG: late arrival detection sensor.

Runs every 30 minutes. Compares the bronze partition record count (from Iceberg
$partitions metadata — O(partitions), not O(rows)) against the latest baseline
stored in lakehouse.qa.bronze_partition_manifest.

For each event_date where the delta exceeds LATE_ARRIVAL_THRESHOLD, triggers
batch_backfill for that date.

To test manually without waiting for the schedule:
    airflow dags trigger late_arrival_sensor
"""

from __future__ import annotations

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from orchestration.airflow_batch_dates import et_datetime
from orchestration.airflow_late_arrival_tasks import detect_and_trigger_late_arrival_backfill

DAG_ID = "late_arrival_sensor"
BACKFILL_DAG_ID = "batch_backfill"


def _trigger_backfill_for_date(event_date: str) -> None:
    trigger_dag(
        dag_id=BACKFILL_DAG_ID,
        conf={"start_date": event_date, "end_date": event_date},
        replace_microseconds=False,
    )


def run_detection() -> None:
    detect_and_trigger_late_arrival_backfill(
        trigger_dag_run_fn=_trigger_backfill_for_date,
    )


with DAG(
    dag_id=DAG_ID,
    description=(
        "Periodic sensor that detects late-arriving bronze events and triggers "
        "batch_backfill for affected dates. Uses Iceberg $partitions metadata — "
        "no full table scan."
    ),
    schedule="*/30 * * * *",
    start_date=et_datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["batch", "sensor", "late-arrival"],
) as dag:

    PythonOperator(
        task_id="detect_and_trigger",
        python_callable=run_detection,
    )
