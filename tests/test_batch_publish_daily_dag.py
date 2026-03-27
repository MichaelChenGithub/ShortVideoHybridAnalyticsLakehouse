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
        tags: list[str],
    ) -> None:
        self.dag_id = dag_id
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
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

        with patch.dict(
            sys.modules,
            {
                "airflow": airflow_module,
                "airflow.operators": operators_module,
                "airflow.operators.empty": empty_module,
                "airflow.operators.python": python_module,
                "airflow.utils": utils_module,
                "airflow.utils.task_group": task_group_module,
            },
        ):
            return runpy.run_path(str(DAG_PATH))

    def test_dag_module_loads_and_exposes_expected_contract(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.dag_id, "batch_publish_daily")
        self.assertEqual(dag.schedule, "0 8 * * *")
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

    def test_dag_chains_resolution_and_group_order(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        resolve_data_date = dag.tasks["resolve_data_date"]
        self.assertEqual(resolve_data_date.downstream_node_ids, {"conformed-events"})
        self.assertEqual(dag.task_groups["conformed-events"].downstream_node_ids, {"sessionization"})
        self.assertEqual(dag.task_groups["sessionization"].downstream_node_ids, {"batch-gold-metrics"})
        self.assertEqual(dag.task_groups["batch-gold-metrics"].downstream_node_ids, {"quality-gates"})
        self.assertEqual(dag.task_groups["quality-gates"].downstream_node_ids, {"publish-and-evidence"})

    def test_resolve_task_uses_logical_date_template(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        resolve_data_date = dag.tasks["resolve_data_date"]
        self.assertEqual(
            resolve_data_date.kwargs["op_kwargs"],
            {"logical_date": "{{ logical_date.isoformat() }}"},
        )
        self.assertEqual(
            resolve_data_date.kwargs["execution_timeout"],
            timedelta(minutes=5),
        )

    def test_batch_tasks_use_shared_resolved_data_date(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        shared_template = {"data_date": "{{ ti.xcom_pull(task_ids='resolve_data_date') }}"}
        self.assertEqual(
            dag.tasks["build_dim_users_scd2"].kwargs["op_kwargs"],
            {"job_key": "dim_users_scd2", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_dim_videos_scd2"].kwargs["op_kwargs"],
            {"job_key": "dim_videos_scd2", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_events_conformed"].kwargs["op_kwargs"],
            {"job_key": "events_conformed", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_user_activity_sessions_30m"].kwargs["op_kwargs"],
            {"job_key": "user_activity_sessions_30m", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_batch_retention_daily"].kwargs["op_kwargs"],
            {"job_key": "batch_retention_daily", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_batch_engagement_daily"].kwargs["op_kwargs"],
            {"job_key": "batch_engagement_daily", **shared_template},
        )
        self.assertEqual(
            dag.tasks["build_batch_sessionization_daily"].kwargs["op_kwargs"],
            {"job_key": "batch_sessionization_daily", **shared_template},
        )
        self.assertEqual(
            dag.tasks["run_batch_gold_quality_gates"].kwargs["op_kwargs"],
            shared_template,
        )
        for task_id in (
            "build_dim_users_scd2",
            "build_dim_videos_scd2",
            "build_events_conformed",
            "build_user_activity_sessions_30m",
            "build_batch_retention_daily",
            "build_batch_engagement_daily",
            "build_batch_sessionization_daily",
        ):
            self.assertEqual(
                dag.tasks[task_id].kwargs["execution_timeout"],
                timedelta(minutes=30),
            )
        self.assertEqual(
            dag.tasks["run_batch_gold_quality_gates"].kwargs["execution_timeout"],
            timedelta(minutes=10),
        )

    def test_dimension_prerequisites_are_built_after_events_conformed(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.tasks["build_events_conformed"].downstream_node_ids,
            {"build_dim_users_scd2", "build_dim_videos_scd2"},
        )
        self.assertIn(
            "build_events_conformed",
            dag.tasks["build_dim_users_scd2"].upstream_node_ids,
        )
        self.assertIn(
            "build_events_conformed",
            dag.tasks["build_dim_videos_scd2"].upstream_node_ids,
        )

    def test_publish_tasks_remain_explicitly_deferred(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        shared_template = "{{ ti.xcom_pull(task_ids='resolve_data_date') }}"
        self.assertEqual(
            dag.tasks["write_batch_publish_manifest"].kwargs["op_kwargs"],
            {"task_name": "write_batch_publish_manifest", "data_date": shared_template},
        )
        self.assertEqual(
            dag.tasks["emit_publish_ready_signal"].kwargs["op_kwargs"],
            {"task_name": "emit_publish_ready_signal", "data_date": shared_template},
        )
        self.assertEqual(
            dag.tasks["package_run_evidence"].kwargs["op_kwargs"],
            {"task_name": "package_run_evidence", "data_date": shared_template},
        )
        for task_id in (
            "write_batch_publish_manifest",
            "emit_publish_ready_signal",
            "package_run_evidence",
        ):
            self.assertEqual(
                dag.tasks[task_id].kwargs["execution_timeout"],
                timedelta(minutes=2),
            )


if __name__ == "__main__":
    unittest.main()
