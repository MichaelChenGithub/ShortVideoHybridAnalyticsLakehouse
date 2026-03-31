from __future__ import annotations

import unittest
from pathlib import Path

import hcl2

TF_PATH = Path(__file__).resolve().parents[1] / "terraform" / "messaging.tf"


def _load() -> dict:
    with TF_PATH.open() as fh:
        return hcl2.load(fh)


class MessagingTfResourceTypeTests(unittest.TestCase):
    """Ensure the file uses MSK Serverless, not a provisioned cluster."""

    def test_serverless_cluster_present(self) -> None:
        parsed = _load()
        resources = parsed.get("resource", [])
        self.assertTrue(
            any("aws_msk_serverless_cluster" in r for r in resources),
            "Expected aws_msk_serverless_cluster resource",
        )

    def test_no_provisioned_cluster(self) -> None:
        parsed = _load()
        resources = parsed.get("resource", [])
        self.assertFalse(
            any("aws_msk_cluster" in r for r in resources),
            "aws_msk_cluster (provisioned) must not be present — use serverless",
        )

    def test_no_msk_configuration(self) -> None:
        # aws_msk_configuration is not supported on serverless
        parsed = _load()
        resources = parsed.get("resource", [])
        self.assertFalse(
            any("aws_msk_configuration" in r for r in resources),
            "aws_msk_configuration is not supported on MSK Serverless",
        )


class MessagingTfAuthTests(unittest.TestCase):
    """Serverless requires IAM auth — plaintext is not allowed."""

    def _cluster(self) -> dict:
        parsed = _load()
        for block in parsed.get("resource", []):
            if "aws_msk_serverless_cluster" in block:
                return list(block["aws_msk_serverless_cluster"].values())[0]
        self.fail("aws_msk_serverless_cluster not found")

    def test_iam_auth_enabled(self) -> None:
        cluster = self._cluster()
        iam_enabled = (
            cluster["client_authentication"][0]
            ["sasl"][0]
            ["iam"][0]
            ["enabled"]
        )
        self.assertTrue(iam_enabled, "SASL/IAM auth must be enabled on MSK Serverless")


class MessagingTfSecurityGroupTests(unittest.TestCase):
    """Port 9098 is the SASL/IAM port — 9092/9094 are plaintext/TLS-only."""

    def _ingress_rule(self) -> dict:
        parsed = _load()
        for block in parsed.get("resource", []):
            if "aws_security_group_rule" in block:
                for name, rule in block["aws_security_group_rule"].items():
                    if "ingress" in name:
                        return rule
        self.fail("No ingress aws_security_group_rule found")

    def test_ingress_port_is_sasl_iam(self) -> None:
        rule = self._ingress_rule()
        self.assertEqual(rule["from_port"], 9098, "Ingress from_port must be 9098 (SASL/IAM)")
        self.assertEqual(rule["to_port"], 9098, "Ingress to_port must be 9098 (SASL/IAM)")

    def test_no_plaintext_port(self) -> None:
        rule = self._ingress_rule()
        self.assertNotEqual(rule["from_port"], 9092, "Port 9092 (plaintext) not allowed on Serverless")

    def test_no_tls_only_port(self) -> None:
        rule = self._ingress_rule()
        self.assertNotEqual(rule["from_port"], 9094, "Port 9094 (TLS-only) not allowed on Serverless")


class MessagingTfOutputTests(unittest.TestCase):
    """Broker endpoint output must reference the serverless SASL/IAM attribute."""

    def test_sasl_iam_output_present(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertTrue(
            any("msk_bootstrap_brokers_sasl_iam" in o for o in outputs),
            "Output msk_bootstrap_brokers_sasl_iam must be exported",
        )

    def test_no_plaintext_broker_output(self) -> None:
        parsed = _load()
        outputs = parsed.get("output", [])
        self.assertFalse(
            any("msk_bootstrap_brokers" in o and "sasl_iam" not in list(o.keys())[0]
                for o in outputs),
            "Plaintext broker output (msk_bootstrap_brokers) must not be exported",
        )


if __name__ == "__main__":
    unittest.main()
