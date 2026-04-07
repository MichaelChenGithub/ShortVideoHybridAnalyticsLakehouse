"""Airflow DAG for manual batch backfill over a date range.

Trigger via the Airflow UI or CLI:
    airflow dags trigger batch_backfill \\
        --conf '{"start_date": "2026-03-01", "end_date": "2026-03-05"}'

Each date is processed by triggering one run of batch_publish_daily, passing
the target date via the data_date param override. This means backfill uses the
identical task graph, retry logic, WAP branch management, and publish/evidence
steps as the scheduled daily pipeline — no separate code path to maintain.

Runs are sequential (max_active_tis_per_dagrun=1) and wait_for_completion=True
so a failing date halts the backfill before advancing to the next.
"""

from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from orchestration.airflow_batch_dates import et_datetime
from orchestration.airflow_backfill_tasks import build_trigger_confs as _build_confs

DAG_ID = "batch_backfill"

default_args = {
    "retries": 0,  # backfill runs must not auto-retry — operator decides resume point
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Manual backfill — triggers batch_publish_daily once per date in range. "
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

    @task
    def build_trigger_confs(start_date: str, end_date: str) -> list[dict]:
        return _build_confs(start_date, end_date)

    confs = build_trigger_confs(
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}",
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_daily_pipeline",
        trigger_dag_id="batch_publish_daily",
        wait_for_completion=True,
        reset_dag_run=True,
        max_active_tis_per_dagrun=1,
    ).expand(conf=confs)
