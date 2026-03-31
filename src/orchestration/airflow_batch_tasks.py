"""Helpers for executing local Airflow batch tasks in the shared Spark container."""

from __future__ import annotations

import os
import re
import subprocess
from typing import Iterable

SPARK_CONTAINER = "lakehouse-spark"
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"
SPARK_SRC_ROOT = "/home/iceberg/local/src/spark"
SCRIPT_SRC_ROOT = "/home/iceberg/local/src/scripts"
BRANCH_LIFECYCLE_SCRIPT = f"{SPARK_SRC_ROOT}/bt_branch_lifecycle.py"
DBT_PROJECT_DIR = "/home/iceberg/local/repo"

SPARK_BATCH_SPECS = {
    "dim_users_scd2": {
        "path": f"{SPARK_SRC_ROOT}/bt_dim_users_scd2.py",
        "data_date_env": None,
    },
    "dim_videos_scd2": {
        "path": f"{SPARK_SRC_ROOT}/bt_dim_videos_scd2.py",
        "data_date_env": None,
    },
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


def slugify_run_id(run_id: str, max_length: int = 63) -> str:
    """Convert an Airflow run_id to a valid Iceberg branch name.

    Replaces any non-alphanumeric character with ``_``, collapses repeated
    underscores, and prefixes with ``run_``.  Truncates to *max_length* to stay
    within catalog identifier limits.
    """
    slug = re.sub(r"[^a-zA-Z0-9]", "_", run_id)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return f"run_{slug}"[:max_length]


def build_spark_submit_command(
    module_path: str,
    *,
    data_date_env: str | None,
    data_date: str,
    wap_branch: str | None = None,
) -> list[str]:
    """Build a docker-exec spark-submit command for one batch job.

    When *wap_branch* is provided, ``--conf spark.wap.branch=<branch>`` is
    injected so the Spark session writes to the WAP branch rather than main.
    """
    command = ["docker", "exec", SPARK_CONTAINER]
    if data_date_env is not None:
        command.extend(["env", f"{data_date_env}={data_date}"])
    command.append(SPARK_SUBMIT_BIN)
    if wap_branch:
        command.extend(["--conf", f"spark.wap.branch={wap_branch}"])
    command.append(module_path)
    return command


def build_branch_lifecycle_command(op: str, branch_name: str) -> list[str]:
    """Build a docker-exec spark-submit command that runs bt_branch_lifecycle.py.

    *op* must be one of ``create``, ``fast_forward``, or ``drop``.
    Environment variables are injected via ``docker exec -e`` so the lifecycle
    script can read them without shell quoting issues.
    """
    return [
        "docker", "exec",
        "-e", f"ICEBERG_BRANCH_OP={op}",
        "-e", f"ICEBERG_WAP_BRANCH={branch_name}",
        SPARK_CONTAINER,
        SPARK_SUBMIT_BIN,
        BRANCH_LIFECYCLE_SCRIPT,
    ]


def build_python_script_command(script_path: str, *, data_date: str) -> list[str]:
    """Build a docker-exec python command for one verifier script."""
    return [
        "docker", "exec", SPARK_CONTAINER,
        "python", script_path,
        "--data-date", data_date,
    ]


def _run_checked(command: Iterable[str], *, label: str, data_date: str = "") -> None:
    command_list = list(command)
    print(f"[AIRFLOW-BATCH] task={label} data_date={data_date} command={' '.join(command_list)}")
    subprocess.run(command_list, check=True)


def run_spark_batch_job(job_key: str, *, data_date: str, wap_branch: str | None = None) -> None:
    """Run one batch Spark job in the shared local Spark container.

    When *wap_branch* is provided the job writes to that branch via
    ``spark.wap.branch`` session config rather than to main.
    """
    spec = SPARK_BATCH_SPECS[job_key]
    _run_checked(
        build_spark_submit_command(
            spec["path"],
            data_date_env=spec["data_date_env"],
            data_date=data_date,
            wap_branch=wap_branch,
        ),
        label=job_key,
        data_date=data_date,
    )


def run_branch_op(op: str, *, branch_name: str) -> None:
    """Execute one Iceberg branch lifecycle operation across all governed tables."""
    _run_checked(
        build_branch_lifecycle_command(op, branch_name),
        label=f"branch_{op}",
    )


def create_iceberg_branch(run_id: str) -> str:
    """Create a run-scoped WAP branch on all governed tables; return the branch name.

    The branch name is derived from the Airflow *run_id* and stored in XCom so
    downstream tasks can reference it via ``ti.xcom_pull(task_ids='create_branch')``.
    """
    branch_name = slugify_run_id(run_id)
    run_branch_op("create", branch_name=branch_name)
    print(f"[AIRFLOW-BATCH] WAP branch created: branch={branch_name} run_id={run_id}")
    return branch_name


def merge_coordinator_task(branch_name: str) -> None:
    """Fast-forward main to the run branch on all governed tables.

    This is the single promotion gate.  It only runs after all upstream
    build and quality-gate tasks succeed (Airflow default ALL_SUCCESS).
    """
    run_branch_op("fast_forward", branch_name=branch_name)
    print(f"[AIRFLOW-BATCH] merge_coordinator: promoted branch={branch_name} to main")


def cleanup_branch_task(branch_name: str) -> None:
    """Drop the run-scoped branch from all governed tables.

    Runs unconditionally (trigger_rule=ALL_DONE) so the branch is always
    cleaned up whether the run succeeded or failed.
    """
    run_branch_op("drop", branch_name=branch_name)
    print(f"[AIRFLOW-BATCH] cleanup: dropped branch={branch_name}")


def emit_publish_ready_signal_task(data_date: str, branch_name: str) -> None:
    """Log the publish-ready signal for downstream DAGs and serving handoff.

    Gated on merge_coordinator success, so this only fires when main has been
    fully promoted.
    """
    print(
        f"[AIRFLOW-BATCH] PUBLISH READY "
        f"data_date={data_date} promoted_branch={branch_name} status=published"
    )


def package_run_evidence_task(data_date: str, branch_name: str) -> None:
    """Log run evidence: Airflow run metadata + branch promotion summary."""
    print(
        f"[AIRFLOW-BATCH] run evidence packaged: "
        f"data_date={data_date} promoted_branch={branch_name}"
    )


def run_gold_quality_gates(*, data_date: str) -> None:
    """Run all publish-critical gold quality gates for one data_date."""
    for script_path in GOLD_QUALITY_GATE_SCRIPTS:
        _run_checked(
            build_python_script_command(script_path, data_date=data_date),
            label=script_path.rsplit("/", 1)[-1],
            data_date=data_date,
        )


def run_dbt_quality_gates(*, data_date: str, wap_branch: str) -> None:
    """Run dbt semantic quality tests against the WAP branch via Trino.

    The ``ICEBERG_WAP_BRANCH`` env var is read by the dbt profile
    (``profiles.yml``) and forwarded to Trino as a session property so that
    all dbt queries target the run branch rather than main.

    dbt severity semantics are preserved: ``warn`` tests are non-blocking,
    ``error`` tests block promotion.
    """
    env = {**os.environ, "ICEBERG_WAP_BRANCH": wap_branch}
    dbt_args = ["--profiles-dir", DBT_PROJECT_DIR, "--project-dir", DBT_PROJECT_DIR]
    print(
        f"[AIRFLOW-BATCH] task=dbt_quality_gates "
        f"data_date={data_date} wap_branch={wap_branch}"
    )
    subprocess.run(["dbt", "run"] + dbt_args, check=True, env=env)
    subprocess.run(["dbt", "test"] + dbt_args, check=True, env=env)
