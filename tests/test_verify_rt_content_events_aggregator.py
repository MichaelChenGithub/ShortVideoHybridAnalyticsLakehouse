from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_rt_content_events_aggregator import (  # noqa: E402
    validate_aggregator_snapshot,
)


class VerifyRtContentEventsAggregatorTests(unittest.TestCase):
    def test_validation_passes_for_healthy_snapshot(self) -> None:
        now_ms = 2_000_000
        snapshot = {
            "raw_count": 100,
            "gold_count": 15,
            "duplicate_key_rows": 0,
            "null_required_rows": 0,
            "negative_metric_rows": 0,
            "max_processed_at_ms": now_ms - 30_000,
        }

        errors = validate_aggregator_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            max_freshness_minutes=2,
        )
        self.assertEqual(errors, [])

    def test_validation_fails_for_row_count_thresholds(self) -> None:
        snapshot = {
            "raw_count": 0,
            "gold_count": 0,
            "duplicate_key_rows": 0,
            "null_required_rows": 0,
            "negative_metric_rows": 0,
            "max_processed_at_ms": None,
        }

        errors = validate_aggregator_snapshot(
            snapshot,
            now_ms=1_000_000,
            min_raw_rows=1,
            min_gold_rows=1,
            max_freshness_minutes=2,
        )

        joined = "\n".join(errors)
        self.assertIn("raw_count below threshold", joined)
        self.assertIn("gold_count below threshold", joined)
        self.assertIn("max_processed_at is null", joined)

    def test_validation_fails_for_integrity_and_freshness(self) -> None:
        now_ms = 3_000_000
        snapshot = {
            "raw_count": 10,
            "gold_count": 10,
            "duplicate_key_rows": 2,
            "null_required_rows": 1,
            "negative_metric_rows": 3,
            "max_processed_at_ms": now_ms - 900_000,
        }

        errors = validate_aggregator_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            max_freshness_minutes=5,
        )

        joined = "\n".join(errors)
        self.assertIn("gold uniqueness violated", joined)
        self.assertIn("required fields contain nulls", joined)
        self.assertIn("metrics contain negative values", joined)
        self.assertIn("freshness breach", joined)

    def test_validation_fails_when_max_processed_at_is_before_run_start(self) -> None:
        now_ms = 4_000_000
        snapshot = {
            "raw_count": 10,
            "gold_count": 10,
            "duplicate_key_rows": 0,
            "null_required_rows": 0,
            "negative_metric_rows": 0,
            "max_processed_at_ms": 3_950_000,
        }

        errors = validate_aggregator_snapshot(
            snapshot,
            now_ms=now_ms,
            min_raw_rows=1,
            min_gold_rows=1,
            max_freshness_minutes=10,
            min_processed_at_ms=3_980_000,
        )

        joined = "\n".join(errors)
        self.assertIn("predates this acceptance run", joined)


if __name__ == "__main__":
    unittest.main()
