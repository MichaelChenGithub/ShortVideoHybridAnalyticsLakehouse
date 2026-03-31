from __future__ import annotations

import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestration.airflow_batch_tasks import (  # noqa: E402
    BRANCH_LIFECYCLE_SCRIPT,
    GOLD_QUALITY_GATE_SCRIPTS,
    SPARK_SUBMIT_BIN,
    _run_emr_job,
    build_branch_lifecycle_command,
    build_python_script_command,
    build_spark_submit_command,
    create_iceberg_branch,
    run_gold_quality_gates,
    run_spark_batch_job,
    slugify_run_id,
)


class SlugifyRunIdTests(unittest.TestCase):
    def test_replaces_special_chars_with_underscores(self) -> None:
        # Consecutive underscores (including Airflow's __ separator) are
        # collapsed to a single underscore — safe Iceberg identifier, still unique.
        self.assertEqual(
            slugify_run_id("scheduled__2026-03-27T08:00:00+00:00"),
            "run_scheduled_2026_03_27T08_00_00_00_00",
        )

    def test_collapses_consecutive_underscores(self) -> None:
        result = slugify_run_id("manual__2026-03-27")
        self.assertNotIn("___", result)

    def test_always_prefixes_with_run(self) -> None:
        self.assertTrue(slugify_run_id("anything").startswith("run_"))

    def test_truncates_to_max_length(self) -> None:
        long_id = "a" * 100
        result = slugify_run_id(long_id, max_length=63)
        self.assertEqual(len(result), 63)

    def test_result_contains_only_safe_chars(self) -> None:
        import re
        result = slugify_run_id("scheduled__2026-03-27T08:00:00+00:00")
        self.assertRegex(result, r"^[a-zA-Z0-9_]+$")


class BuildSparkSubmitCommandTests(unittest.TestCase):
    def test_includes_env_and_script_path(self) -> None:
        command = build_spark_submit_command(
            "/home/iceberg/local/src/spark/bt_events_conformed.py",
            data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
            data_date="2026-03-20",
        )

        self.assertEqual(
            command,
            [
                "docker", "exec", "lakehouse-spark",
                "env", "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
        )

    def test_skips_env_when_data_date_env_is_none(self) -> None:
        command = build_spark_submit_command(
            "/home/iceberg/local/src/spark/bt_dim_users_scd2.py",
            data_date_env=None,
            data_date="2026-03-20",
        )

        self.assertEqual(
            command,
            [
                "docker", "exec", "lakehouse-spark",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_dim_users_scd2.py",
            ],
        )

    def test_injects_wap_branch_conf_between_submit_and_script(self) -> None:
        command = build_spark_submit_command(
            "/home/iceberg/local/src/spark/bt_events_conformed.py",
            data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
            data_date="2026-03-20",
            wap_branch="run_abc123",
        )

        self.assertEqual(
            command,
            [
                "docker", "exec", "lakehouse-spark",
                "env", "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "--conf", "spark.wap.branch=run_abc123",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
        )

    def test_wap_branch_none_produces_same_command_as_no_branch(self) -> None:
        without_branch = build_spark_submit_command(
            "/path/to/script.py",
            data_date_env=None,
            data_date="2026-03-20",
        )
        with_none = build_spark_submit_command(
            "/path/to/script.py",
            data_date_env=None,
            data_date="2026-03-20",
            wap_branch=None,
        )
        self.assertEqual(without_branch, with_none)


class BuildBranchLifecycleCommandTests(unittest.TestCase):
    def test_uses_docker_exec_with_env_flags_before_container(self) -> None:
        command = build_branch_lifecycle_command("create", "run_abc123")

        self.assertEqual(
            command,
            [
                "docker", "exec",
                "-e", "ICEBERG_BRANCH_OP=create",
                "-e", "ICEBERG_WAP_BRANCH=run_abc123",
                "lakehouse-spark",
                SPARK_SUBMIT_BIN,
                BRANCH_LIFECYCLE_SCRIPT,
            ],
        )

    def test_fast_forward_op_is_encoded_correctly(self) -> None:
        command = build_branch_lifecycle_command("fast_forward", "run_abc123")
        self.assertIn("ICEBERG_BRANCH_OP=fast_forward", command)

    def test_drop_op_is_encoded_correctly(self) -> None:
        command = build_branch_lifecycle_command("drop", "run_abc123")
        self.assertIn("ICEBERG_BRANCH_OP=drop", command)

    def test_branch_name_is_passed_as_env_flag(self) -> None:
        command = build_branch_lifecycle_command("create", "run_my_branch")
        self.assertIn("ICEBERG_WAP_BRANCH=run_my_branch", command)


class RunSparkBatchJobTests(unittest.TestCase):
    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_invokes_subprocess_with_expected_command(self, run_mock) -> None:
        run_spark_batch_job("events_conformed", data_date="2026-03-20")

        run_mock.assert_called_once_with(
            [
                "docker", "exec", "lakehouse-spark",
                "env", "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
            check=True,
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_skips_data_date_env_for_dim_jobs(self, run_mock) -> None:
        run_spark_batch_job("dim_users_scd2", data_date="2026-03-20")

        run_mock.assert_called_once_with(
            [
                "docker", "exec", "lakehouse-spark",
                "/opt/spark/bin/spark-submit",
                "/home/iceberg/local/src/spark/bt_dim_users_scd2.py",
            ],
            check=True,
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_passes_wap_branch_conf_when_provided(self, run_mock) -> None:
        run_spark_batch_job(
            "events_conformed",
            data_date="2026-03-20",
            wap_branch="run_abc123",
        )

        run_mock.assert_called_once_with(
            [
                "docker", "exec", "lakehouse-spark",
                "env", "BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
                "/opt/spark/bin/spark-submit",
                "--conf", "spark.wap.branch=run_abc123",
                "/home/iceberg/local/src/spark/bt_events_conformed.py",
            ],
            check=True,
        )

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_propagates_subprocess_failures(self, run_mock) -> None:
        run_mock.side_effect = subprocess.CalledProcessError(1, ["docker"])

        with self.assertRaises(subprocess.CalledProcessError):
            run_spark_batch_job("batch_retention_daily", data_date="2026-03-20")

    @patch("orchestration.airflow_batch_tasks._run_emr_job")
    @patch("orchestration.airflow_batch_tasks.EMR_APPLICATION_ID", "app-123")
    def test_routes_to_aws_and_forwards_wap_branch(self, emr_run_mock) -> None:
        run_spark_batch_job(
            "events_conformed",
            data_date="2026-03-20",
            wap_branch="run_abc123",
        )

        emr_run_mock.assert_called_once_with(
            "events_conformed",
            script_uri="s3://warehouse/scripts/spark/bt_events_conformed.py",
            data_date="2026-03-20",
            data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
            wap_branch="run_abc123",
        )


class RunEmrJobTests(unittest.TestCase):
    @patch("orchestration.airflow_batch_tasks.EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::111111111111:role/emr")
    @patch("orchestration.airflow_batch_tasks.EMR_APPLICATION_ID", "app-123")
    def test_includes_wap_branch_conf_when_provided(self) -> None:
        client = Mock()
        client.start_job_run.return_value = {"jobRunId": "jr-123"}
        client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}
        fake_boto3 = types.SimpleNamespace(client=Mock(return_value=client))

        with patch.dict(sys.modules, {"boto3": fake_boto3}):
            _run_emr_job(
                "events_conformed",
                script_uri="s3://warehouse/scripts/spark/bt_events_conformed.py",
                data_date="2026-03-20",
                data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
                wap_branch="run_abc123",
            )

        client.start_job_run.assert_called_once()
        spark_submit = client.start_job_run.call_args.kwargs["jobDriver"]["sparkSubmit"]
        self.assertEqual(spark_submit["entryPoint"], "s3://warehouse/scripts/spark/bt_events_conformed.py")
        self.assertIn(
            "--conf spark.emr-serverless.driverEnv.BT_EVENTS_CONFORMED_DATA_DATE=2026-03-20",
            spark_submit["sparkSubmitParameters"],
        )
        self.assertIn(
            "--conf spark.wap.branch=run_abc123",
            spark_submit["sparkSubmitParameters"],
        )

    @patch("orchestration.airflow_batch_tasks.EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::111111111111:role/emr")
    @patch("orchestration.airflow_batch_tasks.EMR_APPLICATION_ID", "app-123")
    def test_omits_wap_branch_conf_when_not_provided(self) -> None:
        client = Mock()
        client.start_job_run.return_value = {"jobRunId": "jr-123"}
        client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}
        fake_boto3 = types.SimpleNamespace(client=Mock(return_value=client))

        with patch.dict(sys.modules, {"boto3": fake_boto3}):
            _run_emr_job(
                "events_conformed",
                script_uri="s3://warehouse/scripts/spark/bt_events_conformed.py",
                data_date="2026-03-20",
                data_date_env="BT_EVENTS_CONFORMED_DATA_DATE",
                wap_branch=None,
            )

        spark_submit = client.start_job_run.call_args.kwargs["jobDriver"]["sparkSubmit"]
        self.assertNotIn("spark.wap.branch", spark_submit["sparkSubmitParameters"])


class CreateIcebergBranchTests(unittest.TestCase):
    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_returns_slugified_branch_name(self, run_mock) -> None:
        result = create_iceberg_branch("scheduled__2026-03-27T08:00:00+00:00")

        self.assertTrue(result.startswith("run_"))
        self.assertRegex(result, r"^[a-zA-Z0-9_]+$")

    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_invokes_create_branch_op(self, run_mock) -> None:
        branch_name = create_iceberg_branch("manual__2026-03-27")

        run_mock.assert_called_once()
        cmd = run_mock.call_args.args[0]
        self.assertIn("ICEBERG_BRANCH_OP=create", cmd)
        self.assertIn(f"ICEBERG_WAP_BRANCH={branch_name}", cmd)


class BuildPythonScriptCommandTests(unittest.TestCase):
    def test_uses_verifier_cli_shape(self) -> None:
        command = build_python_script_command(
            "/home/iceberg/local/src/scripts/verify_bt_retention_daily.py",
            data_date="2026-03-20",
        )

        self.assertEqual(
            command,
            [
                "docker", "exec", "lakehouse-spark",
                "python",
                "/home/iceberg/local/src/scripts/verify_bt_retention_daily.py",
                "--data-date", "2026-03-20",
            ],
        )


class RunGoldQualityGatesTests(unittest.TestCase):
    @patch("orchestration.airflow_batch_tasks.subprocess.run")
    def test_runs_all_required_verifiers(self, run_mock) -> None:
        run_gold_quality_gates(data_date="2026-03-20")

        self.assertEqual(run_mock.call_count, len(GOLD_QUALITY_GATE_SCRIPTS))
        self.assertEqual(
            run_mock.call_args_list,
            [
                call(
                    [
                        "docker", "exec", "lakehouse-spark",
                        "python", script_path,
                        "--data-date", "2026-03-20",
                    ],
                    check=True,
                )
                for script_path in GOLD_QUALITY_GATE_SCRIPTS
            ],
        )


if __name__ == "__main__":
    unittest.main()
