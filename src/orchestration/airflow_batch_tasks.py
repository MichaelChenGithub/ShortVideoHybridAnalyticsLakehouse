"""Helpers for executing Airflow batch tasks.

Local dev: runs Spark jobs via docker exec into the shared Spark container.
AWS:       runs Spark jobs via EMR Serverless start_job_run + polling.

Runtime is selected by the presence of the EMR_APPLICATION_ID environment
variable. When set, all batch jobs are submitted to EMR Serverless.
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Iterable

# ── Local dev constants ───────────────────────────────────────────────────────

SPARK_CONTAINER = "lakehouse-spark"
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"
SPARK_SRC_ROOT = "/home/iceberg/local/src/spark"
SCRIPT_SRC_ROOT = "/home/iceberg/local/src/scripts"

# ── AWS constants ─────────────────────────────────────────────────────────────

EMR_APPLICATION_ID = os.environ.get("EMR_APPLICATION_ID")
EMR_EXECUTION_ROLE_ARN = os.environ.get("EMR_EXECUTION_ROLE_ARN")
WAREHOUSE_BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
EMR_SPARK_CONF_URI = f"s3://{WAREHOUSE_BUCKET}/config/spark-defaults-aws.conf"
EMR_SCRIPTS_URI = f"s3://{WAREHOUSE_BUCKET}/scripts/spark"
EMR_POLL_INTERVAL = 15  # seconds

# ── Job registry ──────────────────────────────────────────────────────────────

SPARK_BATCH_SPECS = {
    "dim_users_scd2": {
        "path": f"{SPARK_SRC_ROOT}/bt_dim_users_scd2.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_dim_users_scd2.py",
        "data_date_env": None,
    },
    "dim_videos_scd2": {
        "path": f"{SPARK_SRC_ROOT}/bt_dim_videos_scd2.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_dim_videos_scd2.py",
        "data_date_env": None,
    },
    "events_conformed": {
        "path": f"{SPARK_SRC_ROOT}/bt_events_conformed.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_events_conformed.py",
        "data_date_env": "BT_EVENTS_CONFORMED_DATA_DATE",
    },
    "user_activity_sessions_30m": {
        "path": f"{SPARK_SRC_ROOT}/bt_user_activity_sessions_30m.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_user_activity_sessions_30m.py",
        "data_date_env": "BT_USER_ACTIVITY_SESSIONS_30M_DATA_DATE",
    },
    "batch_retention_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_retention_daily.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_retention_daily.py",
        "data_date_env": "BT_RETENTION_DAILY_DATA_DATE",
    },
    "batch_engagement_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_engagement_daily.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_engagement_daily.py",
        "data_date_env": "BT_ENGAGEMENT_DAILY_DATA_DATE",
    },
    "batch_sessionization_daily": {
        "path": f"{SPARK_SRC_ROOT}/bt_sessionization_daily.py",
        "s3_path": f"{EMR_SCRIPTS_URI}/bt_sessionization_daily.py",
        "data_date_env": "BT_SESSIONIZATION_DAILY_DATA_DATE",
    },
}

GOLD_QUALITY_GATE_SCRIPTS = (
    f"{SCRIPT_SRC_ROOT}/verify_bt_retention_daily.py",
    f"{SCRIPT_SRC_ROOT}/verify_bt_engagement_daily.py",
    f"{SCRIPT_SRC_ROOT}/verify_bt_sessionization_daily.py",
)

# ── Local dev helpers ─────────────────────────────────────────────────────────

def build_spark_submit_command(
    module_path: str,
    *,
    data_date_env: str | None,
    data_date: str,
) -> list[str]:
    """Build a docker-exec spark-submit command for one batch job."""
    command = ["docker", "exec", SPARK_CONTAINER]
    if data_date_env is not None:
        command.extend(["env", f"{data_date_env}={data_date}"])
    command.extend([SPARK_SUBMIT_BIN, module_path])
    return command


def build_python_script_command(script_path: str, *, data_date: str) -> list[str]:
    """Build a docker-exec python command for one verifier script."""
    return [
        "docker", "exec", SPARK_CONTAINER,
        "python", script_path, "--data-date", data_date,
    ]


def _run_checked(command: Iterable[str], *, label: str, data_date: str) -> None:
    command_list = list(command)
    print(f"[AIRFLOW-BATCH] task={label} data_date={data_date} command={' '.join(command_list)}")
    subprocess.run(command_list, check=True)

# ── AWS helper ────────────────────────────────────────────────────────────────

def _run_emr_job(job_key: str, *, script_uri: str, data_date: str, data_date_env: str | None) -> None:
    """Submit one job run to EMR Serverless and poll until terminal state."""
    import boto3
    client = boto3.client("emr-serverless")

    spark_submit_params = (
        f"--conf spark.hadoop.fs.s3a.bucket.{WAREHOUSE_BUCKET}.endpoint=s3.amazonaws.com"
        f" {script_uri}"
    )
    if data_date_env is not None:
        spark_submit_params += f" --conf spark.executorEnv.{data_date_env}={data_date}"

    response = client.start_job_run(
        applicationId=EMR_APPLICATION_ID,
        executionRoleArn=EMR_EXECUTION_ROLE_ARN,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_uri,
                "sparkSubmitParameters": f"--conf spark.emr-serverless.driverEnv.{data_date_env}={data_date}" if data_date_env else "",
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{WAREHOUSE_BUCKET}/emr-logs/{job_key}/"
                }
            }
        },
        name=f"{job_key}_{data_date}",
    )
    run_id = response["jobRunId"]
    print(f"[AIRFLOW-BATCH] task={job_key} data_date={data_date} emr_run_id={run_id}")

    terminal = {"SUCCESS", "FAILED", "CANCELLING", "CANCELLED"}
    while True:
        state = client.get_job_run(
            applicationId=EMR_APPLICATION_ID, jobRunId=run_id
        )["jobRun"]["state"]
        if state == "SUCCESS":
            break
        if state in terminal:
            raise RuntimeError(f"EMR Serverless job {job_key} run {run_id} ended with state {state}")
        time.sleep(EMR_POLL_INTERVAL)

# ── Public API (called by DAG) ────────────────────────────────────────────────

def run_spark_batch_job(job_key: str, *, data_date: str) -> None:
    """Run one batch Spark job — local docker exec or EMR Serverless based on env."""
    spec = SPARK_BATCH_SPECS[job_key]
    if EMR_APPLICATION_ID:
        _run_emr_job(
            job_key,
            script_uri=spec["s3_path"],
            data_date=data_date,
            data_date_env=spec["data_date_env"],
        )
    else:
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
