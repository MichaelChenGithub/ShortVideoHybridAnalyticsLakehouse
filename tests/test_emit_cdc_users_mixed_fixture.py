from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.emit_cdc_users_mixed_fixture import build_mixed_fixture_records  # noqa: E402


class EmitCdcUsersMixedFixtureTests(unittest.TestCase):
    def test_fixture_contains_expected_valid_and_invalid_mix(self) -> None:
        records, summary = build_mixed_fixture_records(
            "fixture_user_001",
            schema_version="m2_v1",
            base_ts_ms=1_900,
        )

        self.assertEqual(len(records), 7)
        self.assertEqual(summary.total_records, 7)
        self.assertEqual(summary.valid_records, 2)
        self.assertEqual(summary.invalid_records, 5)
        self.assertEqual(summary.expected_latest_state, "returning")
        self.assertEqual(summary.expected_latest_region, "LATAM")
        self.assertEqual(summary.expected_latest_ts_ms, 3_900)

    def test_fixture_is_deterministic_for_same_inputs(self) -> None:
        left_records, left_summary = build_mixed_fixture_records("fixture_user_002", base_ts_ms=10_000)
        right_records, right_summary = build_mixed_fixture_records("fixture_user_002", base_ts_ms=10_000)

        self.assertEqual(left_records, right_records)
        self.assertEqual(left_summary, right_summary)


if __name__ == "__main__":
    unittest.main()
