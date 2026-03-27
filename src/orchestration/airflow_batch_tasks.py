"""Helpers for executing local Airflow batch tasks in the shared Spark container."""

from __future__ import annotations

import subprocess
from typing import Iterable

SPARK_CONTAINER = "lakehouse-spark"
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"
SPARK_SRC_ROOT = "/home/iceberg/local/src/spark"
SCRIPT_SRC_ROOT = "/home/iceberg/local/src/scripts"

SPARK_BATCH_SPECS = {
    "events_conformed": {
        "path": f"{SPARK_SRC_ROOT}/bt_events_conformed.py",
        "data_date_env": "BT_EVENTS_CONFORMED_DATA_DATE",
    },
    "user_activity_sessions_30m": {
        "path": f"{SPARK_SRC_ROOT}/bt_user_activity_sessions_30m.py",
        "data_date_env": "BT_USER_ACTIVITY_SESSIONS_30M_DATA_DATE",
    },
    "batch_retention_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_retention_daily.py",
        "data_date_env": "BT_RETENTION_DAILY_DATA_DATE",
    },
    "batch_engagement_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_engagement_daily.py",
        "data_date_env": "BT_ENGAGEMENT_DAILY_DATA_DATE",
    },
    "batch_sessionization_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_sessionization_daily.py",
        "data_date_env": "BT_SESSIONIZATION_DAILY_DATA_DATE",
    },
}

GOLD_QUALITY_GATE_SCRIPTS = (
    f"{SCRIPT_SRC_ROOT}/verify_bt_retention_daily.py",
    f"{SCRIPT_SRC_ROOT}/verify_bt_engagement_daily.py",
    f"{SCRIPT_SRC_ROOT}/verify_bt_sessionization_daily.py",
)


def build_spark_submit_command(module_path: str, *, data_date_env: str, data_date: str) -> list[str]:
    """Build a docker-exec spark-submit command for one batch job."""
    return [
        "docker",
        "exec",
        SPARK_CONTAINER,
        "env",
        f"{data_date_env}={data_date}",
        SPARK_SUBMIT_BIN,
        module_path,
    ]


def build_python_script_command(script_path: str, *, data_date: str) -> list[str]:
    """Build a docker-exec python command for one verifier script."""
    return [
        "docker",
        "exec",
        SPARK_CONTAINER,
        "python",
        script_path,
        "--data-date",
        data_date,
    ]


def _run_checked(command: Iterable[str], *, label: str, data_date: str) -> None:
    command_list = list(command)
    print(f"[AIRFLOW-BATCH] task={label} data_date={data_date} command={' '.join(command_list)}")
    subprocess.run(command_list, check=True)


def run_spark_batch_job(job_key: str, *, data_date: str) -> None:
    """Run one batch Spark job in the shared local Spark container."""
    spec = SPARK_BATCH_SPECS[job_key]
    _run_checked(
        build_spark_submit_command(
            spec["path"],
            data_date_env=spec["data_date_env"],
            data_date=data_date,
        ),
        label=job_key,
        data_date=data_date,
    )


def run_gold_quality_gates(*, data_date: str) -> None:
    """Run all publish-critical gold quality gates for one data_date."""
    for script_path in GOLD_QUALITY_GATE_SCRIPTS:
        _run_checked(
            build_python_script_command(script_path, data_date=data_date),
            label=script_path.rsplit("/", 1)[-1],
            data_date=data_date,
        )


def log_deferred_task(task_name: str, *, data_date: str) -> None:
    """Log the deferred publish/evidence boundary owned by MIC-165."""
    print(
        "[AIRFLOW-BATCH] "
        f"task={task_name} data_date={data_date} deferred_to=MIC-165"
    )
