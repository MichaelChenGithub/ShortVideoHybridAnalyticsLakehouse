from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_sessionization_daily import (  # noqa: E402
    validate_batch_sessionization_daily_rows,
)


class VerifyBtSessionizationDailyTests(unittest.TestCase):
    def test_validate_batch_sessionization_daily_rows_accepts_valid_rows(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "sessions": 3,
                "sessions_per_user": 1.5,
                "avg_session_duration_sec": 42.0,
                "events_per_session": 4.0,
                "watch_time_per_session_ms": 1800.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
            {
                "data_date": "2026-03-20",
                "category": "unknown",
                "region": "unknown",
                "new_vs_returning_user": "unknown",
                "sessions": 0,
                "sessions_per_user": 0.0,
                "avg_session_duration_sec": 0.0,
                "events_per_session": 0.0,
                "watch_time_per_session_ms": 0.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
        ]

        errors = validate_batch_sessionization_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertEqual(errors, [])

    def test_validate_batch_sessionization_daily_rows_catches_duplicate_grain(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "sessions": 3,
                "sessions_per_user": 1.5,
                "avg_session_duration_sec": 42.0,
                "events_per_session": 4.0,
                "watch_time_per_session_ms": 1800.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "sessions": 4,
                "sessions_per_user": 2.0,
                "avg_session_duration_sec": 43.0,
                "events_per_session": 5.0,
                "watch_time_per_session_ms": 2000.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
        ]

        errors = validate_batch_sessionization_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("Duplicate sessionization grain rows found" in err for err in errors))

    def test_validate_batch_sessionization_daily_rows_enforces_governed_user_states(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "brand_new",
                "sessions": 3,
                "sessions_per_user": 1.5,
                "avg_session_duration_sec": 42.0,
                "events_per_session": 4.0,
                "watch_time_per_session_ms": 1800.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
        ]

        errors = validate_batch_sessionization_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("new_vs_returning_user must be one of" in err for err in errors))

    def test_validate_batch_sessionization_daily_rows_requires_published_at(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "sessions": 3,
                "sessions_per_user": 1.5,
                "avg_session_duration_sec": 42.0,
                "events_per_session": 4.0,
                "watch_time_per_session_ms": 1800.0,
                "published_at": None,
            },
        ]

        errors = validate_batch_sessionization_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertEqual(errors, ["published_at must be non-null"])

    def test_validate_batch_sessionization_daily_rows_rejects_negative_metrics(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "sessions": -1,
                "sessions_per_user": -0.5,
                "avg_session_duration_sec": -1.0,
                "events_per_session": -2.0,
                "watch_time_per_session_ms": -10.0,
                "published_at": "2026-03-21T08:00:00Z",
            },
        ]

        errors = validate_batch_sessionization_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("sessions must be non-negative" in err for err in errors))
        self.assertTrue(any("sessions_per_user must be non-negative" in err for err in errors))
        self.assertTrue(any("avg_session_duration_sec must be non-negative" in err for err in errors))
        self.assertTrue(any("events_per_session must be non-negative" in err for err in errors))
        self.assertTrue(any("watch_time_per_session_ms must be non-negative" in err for err in errors))

    def test_validate_batch_sessionization_daily_rows_enforces_min_count(self) -> None:
        errors = validate_batch_sessionization_daily_rows(
            [],
            expected_data_date="2026-03-20",
            min_row_count=2,
        )
        self.assertEqual(
            errors,
            ["Expected at least 2 rows for data_date=2026-03-20, found 0"],
        )
