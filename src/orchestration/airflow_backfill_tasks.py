"""Task callables for the batch_backfill DAG.

All Spark/dbt/branch helpers are imported from airflow_batch_tasks — no logic
is duplicated here. This module owns only the backfill-specific orchestration:
date validation and the sequential per-date loop.
"""

from __future__ import annotations

from orchestration.airflow_batch_dates import date_range
from orchestration.airflow_batch_tasks import (
    cleanup_branch_task,
    create_iceberg_branch,
    merge_coordinator_task,
    run_dbt_quality_gates,
    run_spark_batch_job,
)

# Ordered sequence of Spark jobs — must match the dependency order in the daily DAG.
_SPARK_JOB_SEQUENCE = (
    "events_conformed",
    "dim_users_scd2",
    "dim_videos_scd2",
    "user_activity_sessions_30m",
    "batch_sessionization_daily",
    "batch_retention_daily",
    "batch_engagement_daily",
)


def validate_date_range_task(start_date: str, end_date: str) -> list[str]:
    """Validate params and return the ordered list of dates to reprocess.

    The return value is automatically pushed to XCom by PythonOperator and
    pulled by run_backfill_sequential_task.
    """
    dates = date_range(start_date, end_date)
    print(
        f"[BACKFILL] Validated {len(dates)} date(s) to reprocess: "
        f"{start_date} → {end_date}"
    )
    return dates


def run_backfill_sequential_task(ti=None, **context) -> None:
    """Process each date in the validated range through the full batch pipeline.

    Dates are pulled from XCom (pushed by validate_date_range_task) and
    processed strictly in chronological order. cleanup_branch_task always runs
    via try/finally to prevent stale WAP branches accumulating on failure.

    If any date fails, the loop stops immediately and the exception propagates.
    Re-trigger the DAG from the failed date to resume.
    """
    dates: list[str] = ti.xcom_pull(task_ids="validate_date_range")
    for data_date in dates:
        branch_name = create_iceberg_branch(run_id=f"backfill_{data_date}")
        try:
            for job_key in _SPARK_JOB_SEQUENCE:
                run_spark_batch_job(
                    job_key, data_date=data_date, wap_branch=branch_name
                )
            run_dbt_quality_gates(data_date=data_date, wap_branch=branch_name)
            merge_coordinator_task(branch_name=branch_name)
            print(f"[BACKFILL] Completed date={data_date} branch={branch_name}")
        finally:
            cleanup_branch_task(branch_name=branch_name)
