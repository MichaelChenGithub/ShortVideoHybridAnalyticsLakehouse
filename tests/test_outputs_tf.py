from __future__ import annotations

import unittest
from pathlib import Path

import hcl2

TF_PATH = Path(__file__).resolve().parents[1] / "terraform" / "outputs.tf"


def _load() -> dict:
    with TF_PATH.open() as fh:
        return hcl2.load(fh)


class OutputsTfAthenaTests(unittest.TestCase):
    """Athena workgroup must be provisioned and wired to the checkpoints bucket."""

    def test_athena_workgroup_present(self) -> None:
        parsed = _load()
        resources = parsed.get("resource", [])
        self.assertTrue(
            any("aws_athena_workgroup" in r for r in resources),
            "aws_athena_workgroup must be defined",
        )

    def test_athena_results_routed_to_checkpoints_bucket(self) -> None:
        # Results must not land in the warehouse bucket
        text = TF_PATH.read_text()
        self.assertIn("checkpoints", text,
                      "Athena results output_location must reference the checkpoints bucket")
        self.assertNotIn("warehouse.bucket}/athena", text,
                         "Athena results must not be routed to the warehouse bucket")

    def test_athena_workgroup_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(
            any("athena_workgroup_name" in o for o in outputs),
        )


class OutputsTfStorageOutputsTests(unittest.TestCase):
    """S3 bucket names and Glue DB must be exported for benchmark runs."""

    def test_warehouse_bucket_output(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(any("warehouse_bucket" in o for o in outputs))

    def test_checkpoints_bucket_output(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(any("checkpoints_bucket" in o for o in outputs))

    def test_glue_database_output(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(any("glue_database_name" in o for o in outputs))


class OutputsTfNoBatchTfDependencyTests(unittest.TestCase):
    """outputs.tf must not reference any Glue ETL job resources (batch.tf eliminated)."""

    def test_no_glue_job_reference(self) -> None:
        text = TF_PATH.read_text()
        self.assertNotIn("aws_glue_job", text,
                         "batch.tf is eliminated — no Glue ETL job references allowed")

    def test_vpc_id_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(any("vpc_id" in o for o in outputs))


if __name__ == "__main__":
    unittest.main()
