from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_user_activity_sessions_30m import (  # noqa: E402
    validate_user_activity_sessions_30m_rows,
)


class VerifyBtUserActivitySessions30mTests(unittest.TestCase):
    def test_validate_rows_accepts_valid_rows(self) -> None:
        rows = [
            {
                "session_id": "s_001",
                "user_id": "u_001",
                "session_start_ts": datetime(2026, 3, 20, 12, 0, 0),
                "session_end_ts": datetime(2026, 3, 20, 12, 8, 0),
                "category": "sports",
                "region": "NA",
                "new_vs_returning_user": "returning",
                "session_duration_sec": 480,
                "event_count": 4,
                "watch_time_sum_ms": 27000,
                "data_date": "2026-03-20",
            }
        ]

        errors = validate_user_activity_sessions_30m_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertEqual(errors, [])

    def test_validate_rows_catches_duplicate_ids_and_invalid_metrics(self) -> None:
        rows = [
            {
                "session_id": "s_dup",
                "user_id": "u_001",
                "session_start_ts": datetime(2026, 3, 20, 12, 0, 0),
                "session_end_ts": datetime(2026, 3, 20, 11, 59, 59),
                "category": "sports",
                "region": "NA",
                "new_vs_returning_user": "new",
                "session_duration_sec": -1,
                "event_count": 0,
                "watch_time_sum_ms": -5,
                "data_date": "2026-03-20",
            },
            {
                "session_id": "s_dup",
                "user_id": "u_002",
                "session_start_ts": datetime(2026, 3, 20, 12, 0, 0),
                "session_end_ts": datetime(2026, 3, 20, 12, 10, 0),
                "category": "music",
                "region": "EU",
                "new_vs_returning_user": "power_user",
                "session_duration_sec": 600,
                "event_count": 3,
                "watch_time_sum_ms": 12000,
                "data_date": "2026-03-19",
            },
        ]

        errors = validate_user_activity_sessions_30m_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        joined = "\n".join(errors)
        self.assertIn("session_end_ts must be >=", joined)
        self.assertIn("session_duration_sec must be non-negative", joined)
        self.assertIn("event_count must be >= 1", joined)
        self.assertIn("watch_time_sum_ms must be non-negative", joined)
        self.assertIn("new_vs_returning_user must be one of", joined)
        self.assertIn("data_date mismatch", joined)
        self.assertIn("Duplicate session_id rows found", joined)

    def test_validate_rows_enforces_min_count(self) -> None:
        errors = validate_user_activity_sessions_30m_rows(
            [],
            expected_data_date="2026-03-20",
            min_row_count=2,
        )
        self.assertEqual(
            errors,
            ["Expected at least 2 rows for data_date=2026-03-20, found 0"],
        )


if __name__ == "__main__":
    unittest.main()
