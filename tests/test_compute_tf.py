from __future__ import annotations

import unittest
from pathlib import Path

import hcl2

TF_PATH = Path(__file__).resolve().parents[1] / "terraform" / "compute.tf"


def _load() -> dict:
    with TF_PATH.open() as fh:
        return hcl2.load(fh)


def _resources(parsed: dict) -> list[dict]:
    return parsed.get("resource", [])


def _resource(parsed: dict, resource_type: str) -> dict:
    for block in _resources(parsed):
        if resource_type in block:
            return list(block[resource_type].values())[0]
    raise AssertionError(f"{resource_type} not found in compute.tf")


class ComputeTfEmrServerlessTests(unittest.TestCase):
    """EMR Serverless must be the sole Spark runtime — no EC2 ECS Spark service."""

    def test_emr_serverless_application_present(self) -> None:
        parsed = _load()
        self.assertTrue(
            any("aws_emrserverless_application" in r for r in _resources(parsed)),
            "aws_emrserverless_application must be defined",
        )

    def test_emr_application_type_is_spark(self) -> None:
        parsed = _load()
        app = _resource(parsed, "aws_emrserverless_application")
        self.assertEqual(app["type"], "SPARK")

    def test_emr_zero_initial_capacity(self) -> None:
        # Empty initial_capacity block = no pre-provisioned workers = no idle cost
        parsed = _load()
        app = _resource(parsed, "aws_emrserverless_application")
        capacity = app.get("initial_capacity", [{}])
        # hcl2 parses an empty block as [{}]; a populated block would have keys
        self.assertEqual(capacity, [{}], "initial_capacity must be empty (pay-per-use)")

    def test_no_ec2_autoscaling_group(self) -> None:
        parsed = _load()
        self.assertFalse(
            any("aws_autoscaling_group" in r for r in _resources(parsed)),
            "No EC2 ASG — Spark runs on EMR Serverless, not EC2-backed ECS",
        )

    def test_no_ecs_capacity_provider(self) -> None:
        parsed = _load()
        self.assertFalse(
            any("aws_ecs_capacity_provider" in r for r in _resources(parsed)),
            "No ECS capacity provider — Spark is not an ECS service",
        )

    def test_no_glue_job_resources(self) -> None:
        parsed = _load()
        self.assertFalse(
            any("aws_glue_job" in r for r in _resources(parsed)),
            "Glue ETL jobs eliminated — batch Spark runs via EMR Serverless",
        )

    def test_emr_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(
            any("emr_application_id" in o for o in outputs),
            "emr_application_id output must be exported for Airflow task env injection",
        )


class ComputeTfEcsClusterTests(unittest.TestCase):
    """ECS cluster must exist for Airflow and Metabase (not Spark)."""

    def test_ecs_cluster_present(self) -> None:
        parsed = _load()
        self.assertTrue(
            any("aws_ecs_cluster" in r for r in _resources(parsed)),
            "aws_ecs_cluster must be defined for Airflow + Metabase",
        )

    def test_ecs_cluster_arn_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(
            any("ecs_cluster_arn" in o for o in outputs),
        )


class ComputeTfAirflowTests(unittest.TestCase):
    """Airflow task definition must be Fargate and carry EMR env vars."""

    def _airflow_task_def(self) -> dict:
        parsed = _load()
        return _resource(parsed, "aws_ecs_task_definition")

    def test_airflow_uses_fargate(self) -> None:
        td = self._airflow_task_def()
        self.assertIn("FARGATE", td["requires_compatibilities"])

    def test_airflow_container_has_emr_application_id_env(self) -> None:
        # jsonencode() is opaque to hcl2 — check raw file text
        text = TF_PATH.read_text()
        self.assertIn("EMR_APPLICATION_ID", text,
                      "Airflow container must receive EMR_APPLICATION_ID env var")

    def test_airflow_container_has_emr_execution_role_env(self) -> None:
        text = TF_PATH.read_text()
        self.assertIn("EMR_EXECUTION_ROLE_ARN", text,
                      "Airflow container must receive EMR_EXECUTION_ROLE_ARN env var")


class ComputeTfMetabaseTests(unittest.TestCase):
    """Metabase must be Fargate with a public IP on port 3000."""

    def _metabase_service(self) -> dict:
        parsed = _load()
        for block in _resources(parsed):
            if "aws_ecs_service" in block:
                return list(block["aws_ecs_service"].values())[0]
        self.fail("aws_ecs_service not found")

    def test_metabase_task_def_uses_fargate(self) -> None:
        parsed = _load()
        # There are two task defs; metabase is the second
        task_defs = [
            list(b["aws_ecs_task_definition"].values())[0]
            for b in _resources(parsed)
            if "aws_ecs_task_definition" in b
        ]
        self.assertTrue(
            any("FARGATE" in td["requires_compatibilities"] for td in task_defs),
            "Metabase task definition must use FARGATE",
        )

    def test_metabase_service_launch_type_fargate(self) -> None:
        svc = self._metabase_service()
        self.assertEqual(svc["launch_type"], "FARGATE")

    def test_metabase_service_assign_public_ip(self) -> None:
        svc = self._metabase_service()
        net = svc["network_configuration"][0]
        self.assertTrue(net["assign_public_ip"],
                        "Metabase must have assign_public_ip=true to be publicly reachable")

    def test_metabase_port_3000(self) -> None:
        text = TF_PATH.read_text()
        self.assertIn("containerPort = 3000", text,
                      "Metabase container must expose port 3000")

    def test_metabase_security_group_ingress_port_3000(self) -> None:
        parsed = _load()
        sg = _resource(parsed, "aws_security_group")
        ingress = sg["ingress"][0]
        self.assertEqual(ingress["from_port"], 3000)
        self.assertEqual(ingress["to_port"], 3000)

    def test_metabase_service_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(any("metabase_service_name" in o for o in outputs))


if __name__ == "__main__":
    unittest.main()
