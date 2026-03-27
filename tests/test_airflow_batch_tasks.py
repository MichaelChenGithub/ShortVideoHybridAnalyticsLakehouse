from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestration.airflow_batch_tasks import (  # noqa: E402
    GOLD_QUALITY_GATE_SCRIPTS,
    build_python_script_command,
    build_spark_submit_command,
    run_gold_quality_gates,
    run_spark_batch_job,
)


class AirflowBatchTaskTests(unittest.TestCase):
    def test_build_spark_submit_command_uses_container_and_job_env(self) -> None:
        command = build_spark_submit_command(
            "/home/iceberg/local/src/spark/bt_events_conformed.py",
            data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
            data_date="2026-03-20",
        )

        self.assertEqual(
            command,
            [
                "docker",
                "exec",
                "lakehouse-spark",
                "env",
                "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
        )

    def test_build_python_script_command_uses_verifier_cli_shape(self) -> None:
        command = build_python_script_command(
            "/home/iceberg/local/src/scripts/verify_bt_retention_daily.py",
            data_date="2026-03-20",
        )

        self.assertEqual(
            command,
            [
                "docker",
                "exec",
                "lakehouse-spark",
                "python",
                "/home/iceberg/local/src/scripts/verify_bt_retention_daily.py",
                "--data-date",
                "2026-03-20",
            ],
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_run_spark_batch_job_invokes_subprocess_with_expected_command(self, run_mock) -> None:
        run_spark_batch_job("events_conformed", data_date="2026-03-20")

        run_mock.assert_called_once_with(
            [
                "docker",
                "exec",
                "lakehouse-spark",
                "env",
                "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
            check=True,
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_run_gold_quality_gates_runs_all_required_verifiers(self, run_mock) -> None:
        run_gold_quality_gates(data_date="2026-03-20")

        self.assertEqual(run_mock.call_count, len(GOLD_QUALITY_GATE_SCRIPTS))
        self.assertEqual(
            run_mock.call_args_list,
            [
                call(
                    [
                        "docker",
                        "exec",
                        "lakehouse-spark",
                        "python",
                        script_path,
                        "--data-date",
                        "2026-03-20",
                    ],
                    check=True,
                )
                for script_path in GOLD_QUALITY_GATE_SCRIPTS
            ],
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_run_spark_batch_job_propagates_failures(self, run_mock) -> None:
        run_mock.side_effect = subprocess.CalledProcessError(1, ["docker"])

        with self.assertRaises(subprocess.CalledProcessError):
            run_spark_batch_job("batch_retention_daily", data_date="2026-03-20")


if __name__ == "__main__":
    unittest.main()
