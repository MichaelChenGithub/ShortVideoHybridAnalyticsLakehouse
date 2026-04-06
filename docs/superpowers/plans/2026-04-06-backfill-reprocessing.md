# Batch Backfill & Parameterized Reprocessing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `batch_backfill` Airflow DAG that reprocesses any date range through the full batch pipeline safely and idempotently.

**Architecture:** A new `batch_backfill` DAG (no schedule, triggered manually) accepts `start_date` and `end_date` params, validates the range, then processes each date sequentially through the existing Spark + dbt + WAP merge pipeline. All complex logic lives in `src/orchestration/airflow_backfill_tasks.py`; the DAG file is thin wiring only.

**Tech Stack:** Python 3.10, Apache Airflow 2.x, existing `airflow_batch_tasks.py` helpers (no changes to them), pytest + unittest.mock.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orchestration/airflow_batch_dates.py` | Modify | Add `date_range()` utility + validation |
| `src/orchestration/airflow_backfill_tasks.py` | Create | Task callables: `validate_date_range_task`, `run_backfill_sequential_task` |
| `dags/batch_backfill.py` | Create | DAG wiring — no logic |
| `tests/test_airflow_batch_dates.py` | Create | Unit tests for `date_range()` |
| `tests/test_batch_backfill.py` | Create | Unit tests for backfill task callables |

**No changes to:** `batch_publish_daily.py`, `airflow_batch_tasks.py`, any Spark job.

---

## Task 1: Add `date_range()` to `airflow_batch_dates.py`

**Files:**
- Modify: `src/orchestration/airflow_batch_dates.py`
- Test: `tests/test_airflow_batch_dates.py`

- [ ] **Step 1: Create the test file with failing tests**

Create `tests/test_airflow_batch_dates.py`:

```python
"""Unit tests for airflow_batch_dates utilities."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from orchestration.airflow_batch_dates import date_range


class TestDateRange:
    def test_returns_ordered_list_of_dates(self):
        result = date_range("2026-03-01", "2026-03-03")
        assert result == ["2026-03-01", "2026-03-02", "2026-03-03"]

    def test_single_date_returns_list_of_one(self):
        result = date_range("2026-03-01", "2026-03-01")
        assert result == ["2026-03-01"]

    def test_start_after_end_raises_value_error(self):
        with pytest.raises(ValueError, match="start_date"):
            date_range("2026-03-05", "2026-03-01")

    def test_future_end_date_raises_value_error(self):
        future = (date.today() + timedelta(days=1)).isoformat()
        with pytest.raises(ValueError, match="end_date"):
            date_range("2026-03-01", future)

    def test_today_end_date_raises_value_error(self):
        today = date.today().isoformat()
        with pytest.raises(ValueError, match="end_date"):
            date_range("2026-03-01", today)

    def test_range_over_90_days_raises_value_error(self):
        start = date(2026, 1, 1)
        end = start + timedelta(days=90)  # 91 dates total
        with pytest.raises(ValueError, match="90 days"):
            date_range(start.isoformat(), end.isoformat())

    def test_exactly_90_dates_is_allowed(self):
        start = date(2026, 1, 1)
        end = start + timedelta(days=89)  # 90 dates total
        result = date_range(start.isoformat(), end.isoformat())
        assert len(result) == 90
        assert result[0] == "2026-01-01"
        assert result[-1] == "2026-03-31"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
.venv/bin/python -m pytest tests/test_airflow_batch_dates.py -v
```

Expected: `ImportError` or `AttributeError` — `date_range` does not exist yet.

- [ ] **Step 3: Add `date_range()` to `airflow_batch_dates.py`**

Append to `src/orchestration/airflow_batch_dates.py` (after the existing `canonical_data_date_from_iso_logical_date` function):

```python

def date_range(start_date: str, end_date: str) -> list[str]:
    """Return an ordered list of YYYY-MM-DD strings from start_date to end_date inclusive.

    Raises ValueError if:
    - start_date > end_date
    - end_date >= today (future dates not allowed)
    - the range spans more than 90 dates
    """
    today = date.today()
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(
            f"start_date {start_date} must be <= end_date {end_date}"
        )
    if end >= today:
        raise ValueError(
            f"end_date {end_date} must be before today {today.isoformat()}"
        )
    if (end - start).days + 1 > 90:
        raise ValueError(
            f"Date range exceeds 90 days: {(end - start).days + 1} dates requested"
        )

    result: list[str] = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
.venv/bin/python -m pytest tests/test_airflow_batch_dates.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orchestration/airflow_batch_dates.py tests/test_airflow_batch_dates.py
git commit -m "feat: add date_range() utility with validation to airflow_batch_dates"
```

---

## Task 2: Create `airflow_backfill_tasks.py` with task callables

**Files:**
- Create: `src/orchestration/airflow_backfill_tasks.py`
- Test: `tests/test_batch_backfill.py`

- [ ] **Step 1: Create the test file with failing tests**

Create `tests/test_batch_backfill.py`:

```python
"""Unit tests for backfill task callables."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from orchestration.airflow_backfill_tasks import (
    _SPARK_JOB_SEQUENCE,
    run_backfill_sequential_task,
    validate_date_range_task,
)


# ── validate_date_range_task ──────────────────────────────────────────────────

class TestValidateDateRangeTask:
    def test_returns_ordered_date_list(self):
        result = validate_date_range_task(
            start_date="2026-03-01", end_date="2026-03-03"
        )
        assert result == ["2026-03-01", "2026-03-02", "2026-03-03"]

    def test_propagates_value_error_from_date_range(self):
        with pytest.raises(ValueError):
            validate_date_range_task(
                start_date="2026-03-05", end_date="2026-03-01"
            )


# ── run_backfill_sequential_task ──────────────────────────────────────────────

MODULE = "orchestration.airflow_backfill_tasks"


@pytest.fixture()
def mock_tasks():
    """Patch all airflow_batch_tasks helpers used by the backfill callable."""
    with (
        patch(f"{MODULE}.create_iceberg_branch") as create_branch,
        patch(f"{MODULE}.run_spark_batch_job") as run_spark,
        patch(f"{MODULE}.run_dbt_quality_gates") as run_dbt,
        patch(f"{MODULE}.merge_coordinator_task") as merge,
        patch(f"{MODULE}.cleanup_branch_task") as cleanup,
    ):
        create_branch.return_value = "run_backfill_2026_03_01"
        yield {
            "create_iceberg_branch": create_branch,
            "run_spark_batch_job": run_spark,
            "run_dbt_quality_gates": run_dbt,
            "merge_coordinator_task": merge,
            "cleanup_branch_task": cleanup,
        }


def _make_ti(dates: list[str]) -> MagicMock:
    ti = MagicMock()
    ti.xcom_pull.return_value = dates
    return ti


class TestRunBackfillSequentialTask:
    def test_calls_full_pipeline_for_each_date(self, mock_tasks):
        dates = ["2026-03-01", "2026-03-02"]
        mock_tasks["create_iceberg_branch"].side_effect = [
            "run_backfill_2026_03_01",
            "run_backfill_2026_03_02",
        ]

        run_backfill_sequential_task(ti=_make_ti(dates))

        assert mock_tasks["create_iceberg_branch"].call_count == 2
        assert mock_tasks["run_spark_batch_job"].call_count == len(_SPARK_JOB_SEQUENCE) * 2
        assert mock_tasks["run_dbt_quality_gates"].call_count == 2
        assert mock_tasks["merge_coordinator_task"].call_count == 2
        assert mock_tasks["cleanup_branch_task"].call_count == 2

    def test_spark_jobs_called_in_correct_order(self, mock_tasks):
        run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))

        actual_job_keys = [
            c.args[0]
            for c in mock_tasks["run_spark_batch_job"].call_args_list
        ]
        assert actual_job_keys == list(_SPARK_JOB_SEQUENCE)

    def test_cleanup_runs_even_when_spark_job_fails(self, mock_tasks):
        mock_tasks["run_spark_batch_job"].side_effect = RuntimeError("Spark failed")

        with pytest.raises(RuntimeError, match="Spark failed"):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))

        mock_tasks["cleanup_branch_task"].assert_called_once_with(
            branch_name="run_backfill_2026_03_01"
        )

    def test_cleanup_runs_even_when_dbt_fails(self, mock_tasks):
        mock_tasks["run_dbt_quality_gates"].side_effect = RuntimeError("dbt failed")

        with pytest.raises(RuntimeError, match="dbt failed"):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))

        mock_tasks["cleanup_branch_task"].assert_called_once_with(
            branch_name="run_backfill_2026_03_01"
        )

    def test_loop_stops_on_first_failing_date(self, mock_tasks):
        mock_tasks["run_spark_batch_job"].side_effect = RuntimeError("Spark failed")

        with pytest.raises(RuntimeError):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01", "2026-03-02"]))

        # Only one date attempted
        mock_tasks["create_iceberg_branch"].assert_called_once()

    def test_xcom_pull_uses_correct_task_id(self, mock_tasks):
        ti = _make_ti(["2026-03-01"])
        run_backfill_sequential_task(ti=ti)
        ti.xcom_pull.assert_called_once_with(task_ids="validate_date_range")
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
.venv/bin/python -m pytest tests/test_batch_backfill.py -v
```

Expected: `ImportError` — `airflow_backfill_tasks` does not exist yet.

- [ ] **Step 3: Create `airflow_backfill_tasks.py`**

Create `src/orchestration/airflow_backfill_tasks.py`:

```python
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
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
.venv/bin/python -m pytest tests/test_batch_backfill.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Run the full test suite to confirm no regressions**

```bash
.venv/bin/python -m pytest -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orchestration/airflow_backfill_tasks.py tests/test_batch_backfill.py
git commit -m "feat: add backfill task callables with sequential per-date pipeline loop"
```

---

## Task 3: Create `dags/batch_backfill.py`

**Files:**
- Create: `dags/batch_backfill.py`

No new tests — DAG wiring contains no logic. Verified manually by inspecting the Airflow UI after `make up`.

- [ ] **Step 1: Create the DAG file**

Create `dags/batch_backfill.py`:

```python
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
```

- [ ] **Step 2: Confirm the DAG file parses without errors**

```bash
.venv/bin/python -c "import dags.batch_backfill; print('DAG parses OK')"
```

Expected output: `DAG parses OK`

- [ ] **Step 3: Run the full test suite one final time**

```bash
.venv/bin/python -m pytest -v
```

Expected: all tests PASS, no regressions.

- [ ] **Step 4: Commit**

```bash
git add dags/batch_backfill.py
git commit -m "feat: add batch_backfill DAG with start_date/end_date params"
```
