from __future__ import annotations

import runpy
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
DAG_PATH = REPO_ROOT / "dags" / "batch_publish_daily.py"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class _BaseNode:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.downstream_node_ids: set[str] = set()
        self.upstream_node_ids: set[str] = set()

    def __rshift__(self, other: "_BaseNode") -> "_BaseNode":
        self.downstream_node_ids.add(other.node_id)
        other.upstream_node_ids.add(self.node_id)
        return other


class _FakeDAG:
    current: "_FakeDAG | None" = None

    def __init__(
        self,
        *,
        dag_id: str,
        description: str,
        schedule: str,
        start_date,
        catchup: bool,
        max_active_runs: int,
        default_args: dict | None = None,
        params: dict | None = None,
        tags: list[str],
    ) -> None:
        self.dag_id = dag_id
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.default_args = default_args or {}
        self.params = params or {}
        self.tags = tags
        self.tasks: dict[str, _BaseNode] = {}
        self.task_groups: dict[str, _FakeTaskGroup] = {}

    def __enter__(self) -> "_FakeDAG":
        type(self).current = self
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        type(self).current = None


class _FakeTaskGroup(_BaseNode):
    current: "_FakeTaskGroup | None" = None

    def __init__(self, *, group_id: str) -> None:
        super().__init__(group_id)
        self.group_id = group_id
        dag = _FakeDAG.current
        if dag is None:
            raise AssertionError("TaskGroup created outside DAG context")
        dag.task_groups[group_id] = self

    def __enter__(self) -> "_FakeTaskGroup":
        type(self).current = self
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        type(self).current = None


class _FakeOperator(_BaseNode):
    def __init__(self, *, task_id: str, **kwargs) -> None:
        super().__init__(task_id)
        self.task_id = task_id
        self.kwargs = kwargs
        dag = _FakeDAG.current
        if dag is None:
            raise AssertionError("Operator created outside DAG context")
        dag.tasks[task_id] = self


class _FakeEmptyOperator(_FakeOperator):
    pass


class _FakePythonOperator(_FakeOperator):
    pass


_FAKE_TRIGGER_RULE = types.SimpleNamespace(ALL_DONE="all_done", ALL_SUCCESS="all_success")


class BatchPublishDailyDagTests(unittest.TestCase):
    def _load_dag_module(self) -> dict[str, object]:
        airflow_module = types.ModuleType("airflow")
        airflow_module.DAG = _FakeDAG

        operators_module = types.ModuleType("airflow.operators")

        empty_module = types.ModuleType("airflow.operators.empty")
        empty_module.EmptyOperator = _FakeEmptyOperator

        python_module = types.ModuleType("airflow.operators.python")
        python_module.PythonOperator = _FakePythonOperator

        utils_module = types.ModuleType("airflow.utils")
        task_group_module = types.ModuleType("airflow.utils.task_group")
        task_group_module.TaskGroup = _FakeTaskGroup
        trigger_rule_module = types.ModuleType("airflow.utils.trigger_rule")
        trigger_rule_module.TriggerRule = _FAKE_TRIGGER_RULE

        models_module = types.ModuleType("airflow.models")
        param_module = types.ModuleType("airflow.models.param")
        param_module.Param = lambda default=None, **kwargs: default

        with patch.dict(
            sys.modules,
            {
                "airflow": airflow_module,
                "airflow.models": models_module,
                "airflow.models.param": param_module,
                "airflow.operators": operators_module,
                "airflow.operators.empty": empty_module,
                "airflow.operators.python": python_module,
                "airflow.utils": utils_module,
                "airflow.utils.task_group": task_group_module,
                "airflow.utils.trigger_rule": trigger_rule_module,
            },
        ):
            return runpy.run_path(str(DAG_PATH))

    def test_dag_module_loads_and_exposes_expected_contract(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.dag_id, "batch_publish_daily")
        self.assertEqual(dag.schedule, "0 4 * * *")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(str(dag.start_date.tzinfo), "America/New_York")
        self.assertEqual(
            dag.description,
            "Local DAG for the daily bronze-to-silver through gold D-1 batch path.",
        )

    def test_dag_contains_required_task_groups(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            list(dag.task_groups),
            [
                "conformed-events",
                "sessionization",
                "batch-gold-metrics",
                "quality-gates",
                "publish-and-evidence",
            ],
        )

    def test_resolve_feeds_create_branch_then_conformed_events(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.tasks["resolve_data_date"].downstream_node_ids, {"create_branch"})
        self.assertEqual(dag.tasks["create_branch"].downstream_node_ids, {"conformed-events"})

    def test_sessionization_and_batch_gold_metrics_run_in_parallel(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        # Both groups start after conformed-events
        self.assertEqual(
            dag.task_groups["conformed-events"].downstream_node_ids,
            {"sessionization", "batch-gold-metrics"},
        )
        # Both groups feed into quality-gates
        self.assertEqual(
            dag.task_groups["sessionization"].downstream_node_ids, {"quality-gates"}
        )
        self.assertEqual(
            dag.task_groups["batch-gold-metrics"].downstream_node_ids, {"quality-gates"}
        )

    def test_merge_coordinator_follows_quality_gates(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.task_groups["quality-gates"].downstream_node_ids, {"merge_coordinator"}
        )

    def test_merge_coordinator_fans_out_to_publish_and_cleanup(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.tasks["merge_coordinator"].downstream_node_ids,
            {"publish-and-evidence", "cleanup_branch"},
        )

    def test_cleanup_branch_uses_all_done_trigger_rule(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.tasks["cleanup_branch"].kwargs["trigger_rule"], "all_done")

    def test_resolve_task_uses_logical_date_template(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        resolve_data_date = dag.tasks["resolve_data_date"]
        self.assertEqual(
            resolve_data_date.kwargs["op_kwargs"],
            {
                "logical_date": "{{ logical_date.isoformat() }}",
                "data_date_override": "{{ params.data_date or '' }}",
            },
        )
        self.assertEqual(resolve_data_date.kwargs["execution_timeout"], timedelta(minutes=5))

    def test_create_branch_uses_run_id_template(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.tasks["create_branch"].kwargs["op_kwargs"],
            {"run_id": "{{ run_id }}"},
        )
        self.assertEqual(
            dag.tasks["create_branch"].kwargs["execution_timeout"], timedelta(minutes=10)
        )

    def test_batch_tasks_carry_both_data_date_and_wap_branch_templates(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        wap_base = {
            "data_date": "{{ ti.xcom_pull(task_ids='resolve_data_date') }}",
            "wap_branch": "{{ ti.xcom_pull(task_ids='create_branch') }}",
        }
        for task_id, job_key in [
            ("build_events_conformed", "events_conformed"),
            ("build_dim_users_scd2", "dim_users_scd2"),
            ("build_dim_videos_scd2", "dim_videos_scd2"),
            ("build_user_activity_sessions_30m", "user_activity_sessions_30m"),
            ("build_batch_sessionization_daily", "batch_sessionization_daily"),
            ("build_batch_retention_daily", "batch_retention_daily"),
            ("build_batch_engagement_daily", "batch_engagement_daily"),
        ]:
            with self.subTest(task_id=task_id):
                self.assertEqual(
                    dag.tasks[task_id].kwargs["op_kwargs"],
                    {"job_key": job_key, **wap_base},
                )
                self.assertEqual(
                    dag.tasks[task_id].kwargs["execution_timeout"], timedelta(minutes=30)
                )

    def test_dag_default_args_configure_email_alerts(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertTrue(dag.default_args.get("email_on_failure"))
        self.assertFalse(dag.default_args.get("email_on_retry"))
        self.assertEqual(dag.default_args.get("retries"), 2)

    def test_quality_gate_overrides_retries_to_zero(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.tasks["run_dbt_semantic_quality_tests"].kwargs.get("retries"), 0)

    def test_quality_gate_uses_dbt_callable_with_wap_branch(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        task = dag.tasks["run_dbt_semantic_quality_tests"]
        self.assertEqual(
            task.kwargs["op_kwargs"],
            {
                "data_date": "{{ ti.xcom_pull(task_ids='resolve_data_date') }}",
                "wap_branch": "{{ ti.xcom_pull(task_ids='create_branch') }}",
            },
        )
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(minutes=10))

    def test_dimension_prerequisites_are_built_after_events_conformed(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.tasks["build_events_conformed"].downstream_node_ids,
            {"build_dim_users_scd2", "build_dim_videos_scd2"},
        )

    def test_publish_tasks_are_gated_on_merge_coordinator(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        wap_base = {
            "data_date": "{{ ti.xcom_pull(task_ids='resolve_data_date') }}",
            "branch_name": "{{ ti.xcom_pull(task_ids='create_branch') }}",
        }
        self.assertEqual(dag.tasks["emit_publish_ready_signal"].kwargs["op_kwargs"], wap_base)
        self.assertEqual(dag.tasks["package_run_evidence"].kwargs["op_kwargs"], wap_base)
        for task_id in ("emit_publish_ready_signal", "package_run_evidence"):
            self.assertEqual(
                dag.tasks[task_id].kwargs["execution_timeout"], timedelta(minutes=5)
            )


if __name__ == "__main__":
    unittest.main()
