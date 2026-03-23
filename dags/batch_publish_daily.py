"""Local Airflow DAG scaffold for daily batch publish orchestration."""

from __future__ import annotations

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from orchestration.airflow_batch_dates import (
    ET_TIMEZONE,
    canonical_data_date_from_iso_logical_date,
    et_datetime,
)

DAG_ID = "batch_publish_daily"
DAG_SCHEDULE = "0 8 * * *"
DAG_START_DATE = et_datetime(2026, 3, 1)


def resolve_and_log_data_date(logical_date: str) -> str:
    """Resolve and log the canonical ET D-1 data_date for the DAG run."""
    data_date = canonical_data_date_from_iso_logical_date(logical_date)
    print(f"[AIRFLOW-BATCH] logical_date={logical_date} canonical_data_date={data_date}")
    return data_date


with DAG(
    dag_id=DAG_ID,
    description="Local scaffold for the daily D-1 batch publish path.",
    schedule=DAG_SCHEDULE,
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["batch", "local-dev", "airflow"],
) as dag:
    resolve_data_date = PythonOperator(
        task_id="resolve_data_date",
        python_callable=resolve_and_log_data_date,
        op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    )

    with TaskGroup(group_id="conformed-events") as conformed_events:
        build_events_conformed = EmptyOperator(task_id="build_events_conformed")

    with TaskGroup(group_id="sessionization") as sessionization:
        build_user_activity_sessions_30m = EmptyOperator(task_id="build_user_activity_sessions_30m")
        build_batch_sessionization_daily = EmptyOperator(task_id="build_batch_sessionization_daily")
        build_user_activity_sessions_30m >> build_batch_sessionization_daily

    with TaskGroup(group_id="batch-gold-metrics") as batch_gold_metrics:
        build_batch_retention_daily = EmptyOperator(task_id="build_batch_retention_daily")
        build_batch_engagement_daily = EmptyOperator(task_id="build_batch_engagement_daily")

    with TaskGroup(group_id="quality-gates") as quality_gates:
        run_dbt_semantic_quality_gates = EmptyOperator(task_id="run_dbt_semantic_quality_gates")

    with TaskGroup(group_id="publish-and-evidence") as publish_and_evidence:
        write_batch_publish_manifest = EmptyOperator(task_id="write_batch_publish_manifest")
        emit_publish_ready_signal = EmptyOperator(task_id="emit_publish_ready_signal")
        package_run_evidence = EmptyOperator(task_id="package_run_evidence")
        write_batch_publish_manifest >> emit_publish_ready_signal >> package_run_evidence

    resolve_data_date >> conformed_events >> sessionization >> batch_gold_metrics >> quality_gates >> publish_and_evidence
