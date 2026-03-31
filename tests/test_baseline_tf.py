from __future__ import annotations

import unittest
from pathlib import Path

import hcl2

TF_DIR = Path(__file__).resolve().parents[1] / "terraform"


def _load(filename: str) -> dict:
    with (TF_DIR / filename).open() as fh:
        return hcl2.load(fh)


def _resources(parsed: dict) -> list[dict]:
    return parsed.get("resource", [])


def _resource(parsed: dict, resource_type: str) -> dict:
    for block in _resources(parsed):
        if resource_type in block:
            return list(block[resource_type].values())[0]
    raise AssertionError(f"{resource_type} not found")


class MainTfTests(unittest.TestCase):
    """Provider config and required variables must be present."""

    def test_required_variables_present(self) -> None:
        parsed = _load("main.tf")
        variables = {list(v.keys())[0] for v in parsed.get("variable", [])}
        for name in ("project_name", "aws_region", "airflow_image"):
            self.assertIn(name, variables, f"variable {name!r} must be declared")

    def test_s3_backend_configured(self) -> None:
        text = (TF_DIR / "main.tf").read_text()
        self.assertIn('backend "s3"', text, "S3 remote state backend must be configured")
        self.assertIn("dynamodb_table", text, "DynamoDB lock table must be configured")

    def test_aws_provider_present(self) -> None:
        parsed = _load("main.tf")
        providers = parsed.get("provider", [])
        self.assertTrue(any("aws" in p for p in providers))


class NetworkTfTests(unittest.TestCase):
    """VPC, subnets, NAT, and ECS tasks SG must all be present."""

    def test_vpc_present(self) -> None:
        parsed = _load("network.tf")
        self.assertTrue(any("aws_vpc" in r for r in _resources(parsed)))

    def test_private_and_public_subnets_present(self) -> None:
        parsed = _load("network.tf")
        subnet_names = [
            name
            for block in _resources(parsed)
            if "aws_subnet" in block
            for name in block["aws_subnet"]
        ]
        self.assertIn("private_a", subnet_names)
        self.assertIn("public_a", subnet_names)

    def test_public_subnet_maps_public_ip(self) -> None:
        parsed = _load("network.tf")
        for block in _resources(parsed):
            if "aws_subnet" in block and "public_a" in block["aws_subnet"]:
                self.assertTrue(block["aws_subnet"]["public_a"]["map_public_ip_on_launch"])

    def test_nat_gateway_present(self) -> None:
        parsed = _load("network.tf")
        self.assertTrue(any("aws_nat_gateway" in r for r in _resources(parsed)))

    def test_ecs_tasks_security_group_present(self) -> None:
        parsed = _load("network.tf")
        sg_names = [
            name
            for block in _resources(parsed)
            if "aws_security_group" in block
            for name in block["aws_security_group"]
        ]
        self.assertIn("ecs_tasks", sg_names)

    def test_ecs_tasks_sg_has_unrestricted_egress(self) -> None:
        parsed = _load("network.tf")
        sg = _resource(parsed, "aws_security_group")
        egress = sg["egress"][0]
        self.assertEqual(egress["protocol"], "-1")
        self.assertIn("0.0.0.0/0", egress["cidr_blocks"])


class StorageTfTests(unittest.TestCase):
    """Warehouse and checkpoints S3 buckets and Glue DB must be present."""

    def test_warehouse_bucket_present(self) -> None:
        parsed = _load("storage.tf")
        bucket_names = [
            name
            for block in _resources(parsed)
            if "aws_s3_bucket" in block
            for name in block["aws_s3_bucket"]
        ]
        self.assertIn("warehouse", bucket_names)
        self.assertIn("checkpoints", bucket_names)

    def test_warehouse_versioning_enabled(self) -> None:
        parsed = _load("storage.tf")
        self.assertTrue(
            any("aws_s3_bucket_versioning" in r for r in _resources(parsed)),
            "Warehouse bucket must have versioning enabled",
        )

    def test_glue_catalog_database_present(self) -> None:
        parsed = _load("storage.tf")
        self.assertTrue(any("aws_glue_catalog_database" in r for r in _resources(parsed)))

    def test_buckets_have_force_destroy(self) -> None:
        parsed = _load("storage.tf")
        for block in _resources(parsed):
            if "aws_s3_bucket" in block:
                for name, bucket in block["aws_s3_bucket"].items():
                    self.assertTrue(
                        bucket.get("force_destroy"),
                        f"Bucket {name!r} must have force_destroy=true for demo teardown",
                    )


class IamTfTests(unittest.TestCase):
    """All four IAM roles must be present with correct trust principals."""

    def test_all_four_roles_present(self) -> None:
        parsed = _load("iam.tf")
        role_names = [
            name
            for block in _resources(parsed)
            if "aws_iam_role" in block
            for name in block["aws_iam_role"]
        ]
        for name in ("ecs_task_execution", "ecs_task", "airflow_task", "emr_execution"):
            self.assertIn(name, role_names, f"IAM role {name!r} must be defined")

    def test_emr_execution_role_trusts_emr_serverless(self) -> None:
        text = (TF_DIR / "iam.tf").read_text()
        self.assertIn("emr-serverless.amazonaws.com", text,
                      "emr_execution role must trust emr-serverless.amazonaws.com")

    def test_ecs_roles_trust_ecs_tasks(self) -> None:
        text = (TF_DIR / "iam.tf").read_text()
        self.assertIn("ecs-tasks.amazonaws.com", text,
                      "ECS roles must trust ecs-tasks.amazonaws.com")

    def test_emr_execution_policy_covers_kafka(self) -> None:
        text = (TF_DIR / "iam.tf").read_text()
        self.assertIn("kafka-cluster:", text,
                      "emr_execution policy must include kafka-cluster actions for MSK Serverless")

    def test_airflow_task_can_pass_emr_execution_role(self) -> None:
        text = (TF_DIR / "iam.tf").read_text()
        self.assertIn("iam:PassRole", text,
                      "airflow_task policy must allow iam:PassRole to submit EMR job runs")

    def test_airflow_task_has_emr_serverless_actions(self) -> None:
        text = (TF_DIR / "iam.tf").read_text()
        self.assertIn("emr-serverless:StartJobRun", text)

    def test_ecs_task_execution_uses_managed_policy(self) -> None:
        parsed = _load("iam.tf")
        attachments = [
            list(b["aws_iam_role_policy_attachment"].values())[0]
            for b in _resources(parsed)
            if "aws_iam_role_policy_attachment" in b
        ]
        self.assertTrue(
            any("AmazonECSTaskExecutionRolePolicy" in a.get("policy_arn", "") for a in attachments),
            "ecs_task_execution must attach AmazonECSTaskExecutionRolePolicy",
        )


if __name__ == "__main__":
    unittest.main()
