from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_events_conformed import validate_events_conformed_rows  # noqa: E402


class VerifyBtEventsConformedTests(unittest.TestCase):
    def test_validate_events_conformed_rows_accepts_valid_rows(self) -> None:
        rows = [
            {
                "event_id": "e_001",
                "event_timestamp": "2026-03-19T12:00:00Z",
                "event_date_et": "2026-03-19",
                "data_date": "2026-03-19",
                "video_id": "v_001",
                "user_id": "u_001",
                "event_type": "play_start",
                "watch_time_ms": 3000,
                "category": "unknown",
                "region": "unknown",
            },
            {
                "event_id": "e_002",
                "event_timestamp": "2026-03-19T12:00:10Z",
                "event_date_et": "2026-03-19",
                "data_date": "2026-03-19",
                "video_id": "v_002",
                "user_id": "u_002",
                "event_type": "like",
                "watch_time_ms": 0,
                "category": "unknown",
                "region": "unknown",
            },
        ]

        errors = validate_events_conformed_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertEqual(errors, [])

    def test_validate_events_conformed_rows_catches_duplicates_and_mismatch(self) -> None:
        rows = [
            {
                "event_id": "e_dup",
                "event_timestamp": "2026-03-19T12:00:00Z",
                "event_date_et": "2026-03-18",
                "data_date": "2026-03-19",
                "video_id": "v_001",
                "user_id": "u_001",
                "event_type": "play_start",
                "watch_time_ms": 1000,
                "category": "unknown",
                "region": "unknown",
            },
            {
                "event_id": "e_dup",
                "event_timestamp": "2026-03-19T12:00:10Z",
                "event_date_et": "2026-03-19",
                "data_date": "2026-03-19",
                "video_id": "v_002",
                "user_id": "u_002",
                "event_type": "bad_type",
                "watch_time_ms": -1,
                "category": "unknown",
                "region": "unknown",
            },
        ]

        errors = validate_events_conformed_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertTrue(any("Duplicate event_id rows found" in err for err in errors))
        self.assertTrue(any("event_date_et/data_date mismatch" in err for err in errors))
        self.assertTrue(any("event_type must be one of" in err for err in errors))
        self.assertTrue(any("watch_time_ms must be non-negative" in err for err in errors))

    def test_validate_events_conformed_rows_enforces_min_count(self) -> None:
        errors = validate_events_conformed_rows(
            [],
            expected_data_date="2026-03-19",
            min_row_count=2,
        )
        self.assertEqual(
            errors,
            ["Expected at least 2 rows for data_date=2026-03-19, found 0"],
        )


if __name__ == "__main__":
    unittest.main()
