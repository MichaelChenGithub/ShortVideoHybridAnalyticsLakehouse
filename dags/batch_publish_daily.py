"""Local Airflow DAG for daily batch publish orchestration."""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from orchestration.airflow_batch_tasks import (
    log_deferred_task,
    run_gold_quality_gates,
    run_spark_batch_job,
)
from orchestration.airflow_batch_dates import (
    ET_TIMEZONE,
    canonical_data_date_from_iso_logical_date,
    et_datetime,
)

DAG_ID = "batch_publish_daily"
DAG_SCHEDULE = "0 8 * * *"
DAG_START_DATE = et_datetime(2026, 3, 1)
DATA_DATE_TEMPLATE = "{{ ti.xcom_pull(task_ids='resolve_data_date') }}"
RESOLVE_TIMEOUT = timedelta(minutes=5)
SPARK_TASK_TIMEOUT = timedelta(minutes=30)
QUALITY_GATE_TIMEOUT = timedelta(minutes=10)
DEFERRED_TASK_TIMEOUT = timedelta(minutes=2)


def resolve_and_log_data_date(logical_date: str) -> str:
    """Resolve and log the canonical ET D-1 data_date for the DAG run."""
    data_date = canonical_data_date_from_iso_logical_date(logical_date)
    print(f"[AIRFLOW-BATCH] logical_date={logical_date} canonical_data_date={data_date}")
    return data_date


with DAG(
    dag_id=DAG_ID,
    description="Local DAG for the daily bronze-to-silver through gold D-1 batch path.",
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
        execution_timeout=RESOLVE_TIMEOUT,
    )

    with TaskGroup(group_id="conformed-events") as conformed_events:
        build_dim_users_scd2 = PythonOperator(
            task_id="build_dim_users_scd2",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "dim_users_scd2", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_dim_videos_scd2 = PythonOperator(
            task_id="build_dim_videos_scd2",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "dim_videos_scd2", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_events_conformed = PythonOperator(
            task_id="build_events_conformed",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "events_conformed", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_events_conformed >> build_dim_users_scd2
        build_events_conformed >> build_dim_videos_scd2

    with TaskGroup(group_id="sessionization") as sessionization:
        build_user_activity_sessions_30m = PythonOperator(
            task_id="build_user_activity_sessions_30m",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "user_activity_sessions_30m", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_batch_sessionization_daily = PythonOperator(
            task_id="build_batch_sessionization_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "batch_sessionization_daily", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_user_activity_sessions_30m >> build_batch_sessionization_daily

    with TaskGroup(group_id="batch-gold-metrics") as batch_gold_metrics:
        build_batch_retention_daily = PythonOperator(
            task_id="build_batch_retention_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "batch_retention_daily", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_batch_engagement_daily = PythonOperator(
            task_id="build_batch_engagement_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={"job_key": "batch_engagement_daily", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=SPARK_TASK_TIMEOUT,
        )

    with TaskGroup(group_id="quality-gates") as quality_gates:
        run_batch_gold_quality_gates = PythonOperator(
            task_id="run_batch_gold_quality_gates",
            python_callable=run_gold_quality_gates,
            op_kwargs={"data_date": DATA_DATE_TEMPLATE},
            execution_timeout=QUALITY_GATE_TIMEOUT,
        )

    with TaskGroup(group_id="publish-and-evidence") as publish_and_evidence:
        write_batch_publish_manifest = PythonOperator(
            task_id="write_batch_publish_manifest",
            python_callable=log_deferred_task,
            op_kwargs={"task_name": "write_batch_publish_manifest", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=DEFERRED_TASK_TIMEOUT,
        )
        emit_publish_ready_signal = PythonOperator(
            task_id="emit_publish_ready_signal",
            python_callable=log_deferred_task,
            op_kwargs={"task_name": "emit_publish_ready_signal", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=DEFERRED_TASK_TIMEOUT,
        )
        package_run_evidence = PythonOperator(
            task_id="package_run_evidence",
            python_callable=log_deferred_task,
            op_kwargs={"task_name": "package_run_evidence", "data_date": DATA_DATE_TEMPLATE},
            execution_timeout=DEFERRED_TASK_TIMEOUT,
        )
        write_batch_publish_manifest >> emit_publish_ready_signal >> package_run_evidence

    resolve_data_date >> conformed_events >> sessionization >> batch_gold_metrics >> quality_gates >> publish_and_evidence
