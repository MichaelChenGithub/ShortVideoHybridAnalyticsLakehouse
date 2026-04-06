"""Unit tests for backfill task callables."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestration.airflow_backfill_tasks import (  # noqa: E402
    _SPARK_JOB_SEQUENCE,
    run_backfill_sequential_task,
    validate_date_range_task,
)

MODULE = "orchestration.airflow_backfill_tasks"


def _make_ti(dates: list[str]) -> MagicMock:
    ti = MagicMock()
    ti.xcom_pull.return_value = dates
    return ti


class TestValidateDateRangeTask(unittest.TestCase):
    def test_returns_ordered_date_list(self):
        result = validate_date_range_task(
            start_date="2026-03-01", end_date="2026-03-03"
        )
        self.assertEqual(result, ["2026-03-01", "2026-03-02", "2026-03-03"])

    def test_propagates_value_error_from_date_range(self):
        with self.assertRaises(ValueError):
            validate_date_range_task(
                start_date="2026-03-05", end_date="2026-03-01"
            )


class TestRunBackfillSequentialTask(unittest.TestCase):
    def setUp(self):
        self.patches = {
            "create_iceberg_branch": patch(f"{MODULE}.create_iceberg_branch"),
            "run_spark_batch_job": patch(f"{MODULE}.run_spark_batch_job"),
            "run_dbt_quality_gates": patch(f"{MODULE}.run_dbt_quality_gates"),
            "merge_coordinator_task": patch(f"{MODULE}.merge_coordinator_task"),
            "cleanup_branch_task": patch(f"{MODULE}.cleanup_branch_task"),
        }
        self.mocks = {name: p.start() for name, p in self.patches.items()}
        self.mocks["create_iceberg_branch"].return_value = "run_backfill_2026_03_01"

    def tearDown(self):
        for p in self.patches.values():
            p.stop()

    def test_calls_full_pipeline_for_each_date(self):
        dates = ["2026-03-01", "2026-03-02"]
        self.mocks["create_iceberg_branch"].side_effect = [
            "run_backfill_2026_03_01",
            "run_backfill_2026_03_02",
        ]
        run_backfill_sequential_task(ti=_make_ti(dates))
        self.assertEqual(self.mocks["create_iceberg_branch"].call_count, 2)
        self.assertEqual(self.mocks["run_spark_batch_job"].call_count, len(_SPARK_JOB_SEQUENCE) * 2)
        self.assertEqual(self.mocks["run_dbt_quality_gates"].call_count, 2)
        self.assertEqual(self.mocks["merge_coordinator_task"].call_count, 2)
        self.assertEqual(self.mocks["cleanup_branch_task"].call_count, 2)

    def test_spark_jobs_called_in_correct_order(self):
        run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))
        actual_job_keys = [
            c.args[0]
            for c in self.mocks["run_spark_batch_job"].call_args_list
        ]
        self.assertEqual(actual_job_keys, list(_SPARK_JOB_SEQUENCE))

    def test_cleanup_runs_even_when_spark_job_fails(self):
        self.mocks["run_spark_batch_job"].side_effect = RuntimeError("Spark failed")
        with self.assertRaises(RuntimeError):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))
        self.mocks["cleanup_branch_task"].assert_called_once_with(
            branch_name="run_backfill_2026_03_01"
        )

    def test_cleanup_runs_even_when_dbt_fails(self):
        self.mocks["run_dbt_quality_gates"].side_effect = RuntimeError("dbt failed")
        with self.assertRaises(RuntimeError):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01"]))
        self.mocks["cleanup_branch_task"].assert_called_once_with(
            branch_name="run_backfill_2026_03_01"
        )

    def test_loop_stops_on_first_failing_date(self):
        self.mocks["run_spark_batch_job"].side_effect = RuntimeError("Spark failed")
        with self.assertRaises(RuntimeError):
            run_backfill_sequential_task(ti=_make_ti(["2026-03-01", "2026-03-02"]))
        self.mocks["create_iceberg_branch"].assert_called_once()

    def test_xcom_pull_uses_correct_task_id(self):
        ti = _make_ti(["2026-03-01"])
        run_backfill_sequential_task(ti=ti)
        ti.xcom_pull.assert_called_once_with(task_ids="validate_date_range")


if __name__ == "__main__":
    unittest.main()
