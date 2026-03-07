from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_rt_content_events_contract_enforcement import (  # noqa: E402
    validate_content_contract_snapshot,
)


class VerifyRtContentEventsContractEnforcementTests(unittest.TestCase):
    def test_validation_passes_for_healthy_snapshot(self) -> None:
        now_ms = 2_000_000
        snapshot = {
            "raw_count": 120,
            "gold_count": 12,
            "invalid_count": 8,
            "invalid_required_null_rows": 0,
            "max_invalid_ingested_at_ms": now_ms - 30_000,
        }

        errors = validate_content_contract_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            min_invalid_rows=1,
            max_invalid_rate=0.20,
            max_freshness_minutes=2,
        )
        self.assertEqual(errors, [])

    def test_validation_fails_for_thresholds_and_missing_freshness(self) -> None:
        snapshot = {
            "raw_count": 0,
            "gold_count": 0,
            "invalid_count": 0,
            "invalid_required_null_rows": 0,
            "max_invalid_ingested_at_ms": None,
        }

        errors = validate_content_contract_snapshot(
            snapshot,
            now_ms=1_000_000,
            min_raw_rows=1,
            min_gold_rows=1,
            min_invalid_rows=1,
            max_invalid_rate=0.20,
            max_freshness_minutes=2,
        )

        joined = "\n".join(errors)
        self.assertIn("raw_count below threshold", joined)
        self.assertIn("gold_count below threshold", joined)
        self.assertIn("invalid_count below threshold", joined)
        self.assertIn("max_invalid_ingested_at is null", joined)

    def test_validation_fails_for_invalid_rate_and_null_required_fields(self) -> None:
        now_ms = 3_000_000
        snapshot = {
            "raw_count": 10,
            "gold_count": 5,
            "invalid_count": 10,
            "invalid_required_null_rows": 2,
            "max_invalid_ingested_at_ms": now_ms - 30_000,
        }

        errors = validate_content_contract_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            min_invalid_rows=1,
            max_invalid_rate=0.20,
            max_freshness_minutes=5,
        )

        joined = "\n".join(errors)
        self.assertIn("invalid_rate above threshold", joined)
        self.assertIn("invalid sink has null required fields", joined)

    def test_validation_fails_for_freshness_and_run_scope(self) -> None:
        now_ms = 5_000_000
        snapshot = {
            "raw_count": 100,
            "gold_count": 10,
            "invalid_count": 5,
            "invalid_required_null_rows": 0,
            "max_invalid_ingested_at_ms": now_ms - 900_000,
        }

        errors = validate_content_contract_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            min_invalid_rows=1,
            max_invalid_rate=0.20,
            max_freshness_minutes=5,
            min_ingested_at_ms=4_900_000,
        )

        joined = "\n".join(errors)
        self.assertIn("invalid freshness breach", joined)
        self.assertIn("predates this acceptance run", joined)


if __name__ == "__main__":
    unittest.main()
