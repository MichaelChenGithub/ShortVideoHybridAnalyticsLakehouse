"""Airflow DAG for manual batch backfill over a date range.

Trigger via the Airflow UI or CLI:
    airflow dags trigger batch_backfill \\
        --conf '{"start_date": "2026-03-01", "end_date": "2026-03-05"}'

The DAG processes each date sequentially through the full pipeline:
bronze → silver → gold → dbt quality gates → WAP merge.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from orchestration.airflow_batch_dates import et_datetime
from orchestration.airflow_backfill_tasks import (
    run_backfill_sequential_task,
    validate_date_range_task,
)

DAG_ID = "batch_backfill"

default_args = {
    "retries": 0,  # backfill tasks must not auto-retry — operator decides resume point
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Manual backfill — reprocesses a date range through the full batch pipeline. "
        "Trigger with params: start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)."
    ),
    schedule=None,
    start_date=et_datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "start_date": Param(
            "",
            type="string",
            description="First date to reprocess, inclusive (YYYY-MM-DD).",
        ),
        "end_date": Param(
            "",
            type="string",
            description="Last date to reprocess, inclusive (YYYY-MM-DD).",
        ),
    },
    tags=["batch", "backfill"],
) as dag:

    validate = PythonOperator(
        task_id="validate_date_range",
        python_callable=validate_date_range_task,
        op_kwargs={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
        },
        execution_timeout=timedelta(minutes=2),
    )

    run_backfill = PythonOperator(
        task_id="run_backfill_sequential",
        python_callable=run_backfill_sequential_task,
        execution_timeout=timedelta(hours=12),
    )

    validate >> run_backfill
