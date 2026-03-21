from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_engagement_daily import validate_batch_engagement_daily_rows  # noqa: E402


class VerifyBtEngagementDailyTests(unittest.TestCase):
    def test_validate_batch_engagement_daily_rows_accepts_valid_rows(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "impressions": 10,
                "play_start": 7,
                "play_finish": 5,
                "likes": 2,
                "shares": 1,
                "skips": 1,
                "play_start_rate": 0.7,
                "completion_rate": 5 / 7,
                "interaction_rate": 0.6,
                "skip_rate": 1 / 7,
                "published_at": "2026-03-21T00:00:00Z",
            },
            {
                "data_date": "2026-03-20",
                "category": "music",
                "region": "unknown",
                "new_vs_returning_user": "unknown",
                "impressions": 0,
                "play_start": 0,
                "play_finish": 0,
                "likes": 0,
                "shares": 0,
                "skips": 0,
                "play_start_rate": 0.0,
                "completion_rate": 0.0,
                "interaction_rate": 0.0,
                "skip_rate": 0.0,
                "published_at": "2026-03-21T00:00:00Z",
            },
        ]

        errors = validate_batch_engagement_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertEqual(errors, [])

    def test_validate_batch_engagement_daily_rows_catches_duplicate_grain(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "impressions": 10,
                "play_start": 7,
                "play_finish": 5,
                "likes": 2,
                "shares": 1,
                "skips": 1,
                "play_start_rate": 0.7,
                "completion_rate": 5 / 7,
                "interaction_rate": 0.6,
                "skip_rate": 1 / 7,
                "published_at": "2026-03-21T00:00:00Z",
            },
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "new",
                "impressions": 8,
                "play_start": 4,
                "play_finish": 2,
                "likes": 1,
                "shares": 1,
                "skips": 1,
                "play_start_rate": 0.5,
                "completion_rate": 0.5,
                "interaction_rate": 1.0,
                "skip_rate": 0.25,
                "published_at": "2026-03-21T00:00:00Z",
            },
        ]

        errors = validate_batch_engagement_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("Duplicate grain rows found" in err for err in errors))

    def test_validate_batch_engagement_daily_rows_enforces_governed_user_states(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "brand_new",
                "impressions": 10,
                "play_start": 7,
                "play_finish": 5,
                "likes": 2,
                "shares": 1,
                "skips": 1,
                "play_start_rate": 0.7,
                "completion_rate": 5 / 7,
                "interaction_rate": 0.6,
                "skip_rate": 1 / 7,
                "published_at": "2026-03-21T00:00:00Z",
            }
        ]

        errors = validate_batch_engagement_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("new_vs_returning_user must be one of" in err for err in errors))

    def test_validate_batch_engagement_daily_rows_catches_formula_mismatch(self) -> None:
        rows = [
            {
                "data_date": "2026-03-20",
                "category": "sports",
                "region": "us",
                "new_vs_returning_user": "returning",
                "impressions": 10,
                "play_start": 7,
                "play_finish": 5,
                "likes": 2,
                "shares": 1,
                "skips": 1,
                "play_start_rate": 0.8,
                "completion_rate": 5 / 7,
                "interaction_rate": 0.6,
                "skip_rate": 1 / 7,
                "published_at": "2026-03-21T00:00:00Z",
            }
        ]

        errors = validate_batch_engagement_daily_rows(
            rows,
            expected_data_date="2026-03-20",
            min_row_count=1,
        )
        self.assertTrue(any("play_start_rate mismatch" in err for err in errors))

    def test_validate_batch_engagement_daily_rows_enforces_min_count(self) -> None:
        errors = validate_batch_engagement_daily_rows(
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
