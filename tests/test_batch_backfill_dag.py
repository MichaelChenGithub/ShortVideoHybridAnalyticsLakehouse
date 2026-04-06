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
DAG_PATH = REPO_ROOT / "dags" / "batch_backfill.py"

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
        schedule,
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

    def __enter__(self) -> "_FakeDAG":
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


class _FakePythonOperator(_FakeOperator):
    pass


class _FakeParam:
    def __init__(self, default: str, *, type: str, description: str) -> None:
        self.default = default
        self.type = type
        self.description = description


class BatchBackfillDagTests(unittest.TestCase):
    def _load_dag_module(self) -> dict[str, object]:
        airflow_module = types.ModuleType("airflow")
        airflow_module.DAG = _FakeDAG

        operators_module = types.ModuleType("airflow.operators")

        python_module = types.ModuleType("airflow.operators.python")
        python_module.PythonOperator = _FakePythonOperator

        models_module = types.ModuleType("airflow.models")
        param_module = types.ModuleType("airflow.models.param")
        param_module.Param = _FakeParam

        with patch.dict(
            sys.modules,
            {
                "airflow": airflow_module,
                "airflow.operators": operators_module,
                "airflow.operators.python": python_module,
                "airflow.models": models_module,
                "airflow.models.param": param_module,
            },
        ):
            return runpy.run_path(str(DAG_PATH))

    def test_dag_module_loads_and_exposes_expected_contract(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.dag_id, "batch_backfill")
        self.assertIsNone(dag.schedule)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(str(dag.start_date.tzinfo), "America/New_York")
        self.assertIn("backfill", dag.description.lower())

    def test_dag_has_required_params(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertIn("start_date", dag.params)
        self.assertIn("end_date", dag.params)

    def test_validate_feeds_into_run_backfill(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(
            dag.tasks["validate_date_range"].downstream_node_ids,
            {"run_backfill_sequential"},
        )

    def test_validate_task_uses_param_templates(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        validate_task = dag.tasks["validate_date_range"]
        self.assertEqual(
            validate_task.kwargs["op_kwargs"],
            {
                "start_date": "{{ params.start_date }}",
                "end_date": "{{ params.end_date }}",
            },
        )
        self.assertEqual(validate_task.kwargs["execution_timeout"], timedelta(minutes=2))

    def test_run_backfill_has_adequate_timeout(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        run_backfill_task = dag.tasks["run_backfill_sequential"]
        self.assertEqual(run_backfill_task.kwargs["execution_timeout"], timedelta(hours=12))

    def test_dag_default_args_no_retries(self) -> None:
        module_globals = self._load_dag_module()

        dag = module_globals["dag"]
        self.assertEqual(dag.default_args.get("retries"), 0)


if __name__ == "__main__":
    unittest.main()
