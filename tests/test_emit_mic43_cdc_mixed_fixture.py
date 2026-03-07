from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.emit_mic43_cdc_mixed_fixture import (  # noqa: E402
    DEFAULT_BASE_TS_MS,
    build_mixed_fixture_records,
)


class EmitMic43CdcMixedFixtureTests(unittest.TestCase):
    def test_mixed_fixture_counts_and_expected_final_state(self) -> None:
        records, summary = build_mixed_fixture_records("mic43_vid_001")
        self.assertEqual(len(records), 6)
        self.assertEqual(summary.total_records, 6)
        self.assertEqual(summary.valid_records, 2)
        self.assertEqual(summary.invalid_records, 4)
        self.assertEqual(summary.expected_status, "copyright_strike")
        self.assertEqual(summary.expected_source_ts_ms, DEFAULT_BASE_TS_MS + 2_000)

    def test_fixture_contains_one_malformed_json_record(self) -> None:
        records, _ = build_mixed_fixture_records("mic43_vid_002")
        malformed_count = 0
        for record in records:
            try:
                json.loads(record.raw_value)
            except json.JSONDecodeError:
                malformed_count += 1
        self.assertEqual(malformed_count, 1)

    def test_fixture_is_deterministic_for_same_input(self) -> None:
        left_records, left_summary = build_mixed_fixture_records("mic43_vid_003")
        right_records, right_summary = build_mixed_fixture_records("mic43_vid_003")

        self.assertEqual(left_summary, right_summary)
        self.assertEqual(
            [(record.key, record.raw_value, record.is_valid) for record in left_records],
            [(record.key, record.raw_value, record.is_valid) for record in right_records],
        )


if __name__ == "__main__":
    unittest.main()
