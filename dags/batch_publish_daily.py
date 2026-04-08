"""Local Airflow DAG for daily batch publish orchestration."""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from orchestration.airflow_batch_tasks import (
    cleanup_branch_task,
    create_iceberg_branch,
    emit_publish_ready_signal_task,
    merge_coordinator_task,
    package_run_evidence_task,
    run_dbt_quality_gates,
    run_spark_batch_job,
    write_bronze_partition_manifest_task,
)
from orchestration.airflow_batch_dates import (
    canonical_data_date_from_iso_logical_date,
    et_datetime,
)

DAG_ID = "batch_publish_daily"
DAG_SCHEDULE = "0 4 * * *"
DAG_START_DATE = et_datetime(2026, 3, 1)
DATA_DATE_TEMPLATE = "{{ ti.xcom_pull(task_ids='resolve_data_date') }}"
BRANCH_NAME_TEMPLATE = "{{ ti.xcom_pull(task_ids='create_branch') }}"
RESOLVE_TIMEOUT = timedelta(minutes=5)
BRANCH_TIMEOUT = timedelta(minutes=10)
SPARK_TASK_TIMEOUT = timedelta(minutes=30)
QUALITY_GATE_TIMEOUT = timedelta(minutes=10)
MERGE_TIMEOUT = timedelta(minutes=10)
EVIDENCE_TIMEOUT = timedelta(minutes=5)
PIPELINE_SLA = timedelta(hours=3)

ALERT_EMAIL = os.environ.get("AIRFLOW_ALERT_EMAIL", "ops@example.com")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}


def resolve_and_log_data_date(logical_date: str, data_date_override: str) -> str:
    """Resolve and log the canonical data_date for the DAG run.

    When data_date_override is non-empty (backfill triggered run), it is used
    directly. Otherwise the canonical ET D-1 date is derived from logical_date.
    """
    data_date = data_date_override or canonical_data_date_from_iso_logical_date(logical_date)
    print(f"[AIRFLOW-BATCH] logical_date={logical_date} canonical_data_date={data_date}")
    return data_date


with DAG(
    dag_id=DAG_ID,
    description="Local DAG for the daily bronze-to-silver through gold D-1 batch path.",
    schedule=DAG_SCHEDULE,
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "data_date": Param(
            None,
            type=["null", "string"],
            description=(
                "Override the resolved data_date (backfill use only). "
                "When set, skips the D-1 resolution from logical_date."
            ),
        ),
    },
    tags=["batch", "local-dev", "airflow"],
) as dag:

    # ── Step 0: resolve canonical D-1 data_date ──────────────────────────────
    resolve_data_date = PythonOperator(
        task_id="resolve_data_date",
        python_callable=resolve_and_log_data_date,
        op_kwargs={
            "logical_date": "{{ logical_date.isoformat() }}",
            "data_date_override": "{{ params.data_date or '' }}",
        },
        execution_timeout=RESOLVE_TIMEOUT,
    )

    # ── Step 1: create run-scoped WAP branch on every governed table ──────────
    # The branch name is returned via XCom; downstream tasks reference it via
    # BRANCH_NAME_TEMPLATE so all writes share the same run branch.
    create_branch = PythonOperator(
        task_id="create_branch",
        python_callable=create_iceberg_branch,
        op_kwargs={"run_id": "{{ run_id }}"},
        execution_timeout=BRANCH_TIMEOUT,
    )

    # ── Step 2: bronze → silver (events + SCD2 dims) ─────────────────────────
    with TaskGroup(group_id="conformed-events") as conformed_events:
        build_events_conformed = PythonOperator(
            task_id="build_events_conformed",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "events_conformed",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_dim_users_scd2 = PythonOperator(
            task_id="build_dim_users_scd2",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "dim_users_scd2",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_dim_videos_scd2 = PythonOperator(
            task_id="build_dim_videos_scd2",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "dim_videos_scd2",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_events_conformed >> build_dim_users_scd2
        build_events_conformed >> build_dim_videos_scd2

    # ── Step 3: silver sessions ───────────────────────────────────────────────
    with TaskGroup(group_id="sessionization") as sessionization:
        build_user_activity_sessions_30m = PythonOperator(
            task_id="build_user_activity_sessions_30m",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "user_activity_sessions_30m",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_batch_sessionization_daily = PythonOperator(
            task_id="build_batch_sessionization_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "batch_sessionization_daily",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_user_activity_sessions_30m >> build_batch_sessionization_daily

    # ── Step 4: gold metrics (retention + engagement) ─────────────────────────
    with TaskGroup(group_id="batch-gold-metrics") as batch_gold_metrics:
        build_batch_retention_daily = PythonOperator(
            task_id="build_batch_retention_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "batch_retention_daily",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )
        build_batch_engagement_daily = PythonOperator(
            task_id="build_batch_engagement_daily",
            python_callable=run_spark_batch_job,
            op_kwargs={
                "job_key": "batch_engagement_daily",
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=SPARK_TASK_TIMEOUT,
        )

    # ── Step 5: dbt semantic quality gates against WAP branch ─────────────────
    with TaskGroup(group_id="quality-gates") as quality_gates:
        run_dbt_semantic_quality_tests = PythonOperator(
            task_id="run_dbt_semantic_quality_tests",
            python_callable=run_dbt_quality_gates,
            op_kwargs={
                "data_date": DATA_DATE_TEMPLATE,
                "wap_branch": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=QUALITY_GATE_TIMEOUT,
            retries=0,  # quality gate failures must not retry
        )

    # ── Step 6: promote WAP branch → main (ALL_SUCCESS gate) ─────────────────
    merge_coordinator = PythonOperator(
        task_id="merge_coordinator",
        python_callable=merge_coordinator_task,
        op_kwargs={"branch_name": BRANCH_NAME_TEMPLATE},
        execution_timeout=MERGE_TIMEOUT,
    )

    # ── Step 7a: publish signal + run evidence (gated on merge success) ───────
    with TaskGroup(group_id="publish-and-evidence") as publish_and_evidence:
        emit_publish_ready = PythonOperator(
            task_id="emit_publish_ready_signal",
            python_callable=emit_publish_ready_signal_task,
            op_kwargs={
                "data_date": DATA_DATE_TEMPLATE,
                "branch_name": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=EVIDENCE_TIMEOUT,
        )
        write_manifest = PythonOperator(
            task_id="write_bronze_partition_manifest",
            python_callable=write_bronze_partition_manifest_task,
            op_kwargs={
                "data_date": DATA_DATE_TEMPLATE,
                "dag_run_id": "{{ run_id }}",
            },
            execution_timeout=EVIDENCE_TIMEOUT,
        )
        package_evidence = PythonOperator(
            task_id="package_run_evidence",
            python_callable=package_run_evidence_task,
            op_kwargs={
                "data_date": DATA_DATE_TEMPLATE,
                "branch_name": BRANCH_NAME_TEMPLATE,
            },
            execution_timeout=EVIDENCE_TIMEOUT,
            sla=PIPELINE_SLA,
        )
        emit_publish_ready >> write_manifest >> package_evidence

    # ── Step 7b: drop run branch (ALL_DONE — always runs) ────────────────────
    cleanup_branch = PythonOperator(
        task_id="cleanup_branch",
        python_callable=cleanup_branch_task,
        op_kwargs={"branch_name": BRANCH_NAME_TEMPLATE},
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=BRANCH_TIMEOUT,
    )

    # ── Pipeline wiring ───────────────────────────────────────────────────────
    # sessionization and batch_gold_metrics are independent and run in parallel
    # after conformed_events; quality_gates waits for both to complete.
    resolve_data_date >> create_branch
    create_branch >> conformed_events
    conformed_events >> sessionization >> quality_gates
    conformed_events >> batch_gold_metrics >> quality_gates
    quality_gates >> merge_coordinator
    merge_coordinator >> publish_and_evidence
    merge_coordinator >> cleanup_branch
