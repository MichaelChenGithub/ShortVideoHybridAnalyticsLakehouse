from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_retention_daily import validate_batch_retention_daily_rows  # noqa: E402


class VerifyBtRetentionDailyTests(unittest.TestCase):
    def test_validate_batch_retention_daily_rows_accepts_valid_rows(self) -> None:
        rows = [
            {
                "cohort_date": "2026-03-18",
                "day_n": 1,
                "category": "sports",
                "region": "US",
                "new_vs_returning_user": "new",
                "cohort_users": 10,
                "retained_users": 3,
                "retention_rate": 0.3,
                "data_date": "2026-03-19",
                "published_at": "2026-03-20T08:00:00Z",
            },
            {
                "cohort_date": "2026-03-12",
                "day_n": 7,
                "category": "unknown",
                "region": "unknown",
                "new_vs_returning_user": "unknown",
                "cohort_users": 4,
                "retained_users": 0,
                "retention_rate": 0.0,
                "data_date": "2026-03-19",
                "published_at": "2026-03-20T08:00:00Z",
            },
        ]

        errors = validate_batch_retention_daily_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertEqual(errors, [])

    def test_validate_batch_retention_daily_rows_catches_duplicate_bad_day_and_bad_segment(self) -> None:
        rows = [
            {
                "cohort_date": "2026-03-18",
                "day_n": 2,
                "category": "sports",
                "region": "US",
                "new_vs_returning_user": "stale",
                "cohort_users": 10,
                "retained_users": 3,
                "retention_rate": 0.3,
                "data_date": "2026-03-19",
                "published_at": "2026-03-20T08:00:00Z",
            },
            {
                "cohort_date": "2026-03-18",
                "day_n": 2,
                "category": "sports",
                "region": "US",
                "new_vs_returning_user": "stale",
                "cohort_users": 10,
                "retained_users": 11,
                "retention_rate": 0.9,
                "data_date": "2026-03-18",
                "published_at": "2026-03-20T08:00:00Z",
            },
        ]

        errors = validate_batch_retention_daily_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertTrue(any("day_n must be one of" in err for err in errors))
        self.assertTrue(any("new_vs_returning_user must be one of" in err for err in errors))
        self.assertTrue(any("retained_users must be <= cohort_users" in err for err in errors))
        self.assertTrue(any("data_date mismatch" in err for err in errors))
        self.assertTrue(any("Duplicate retention grain rows found" in err for err in errors))

    def test_validate_batch_retention_daily_rows_requires_null_for_non_computable_rate(self) -> None:
        rows = [
            {
                "cohort_date": "2026-03-18",
                "day_n": 1,
                "category": "sports",
                "region": "US",
                "new_vs_returning_user": "new",
                "cohort_users": 0,
                "retained_users": 0,
                "retention_rate": 0.0,
                "data_date": "2026-03-19",
                "published_at": "2026-03-20T08:00:00Z",
            }
        ]

        errors = validate_batch_retention_daily_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertEqual(
            errors,
            [
                "retention_rate must be NULL when cohort_users = 0 for key="
                "2026-03-18|1|sports|US|new"
            ],
        )

    def test_validate_batch_retention_daily_rows_checks_rate_formula(self) -> None:
        rows = [
            {
                "cohort_date": "2026-03-18",
                "day_n": 1,
                "category": "sports",
                "region": "US",
                "new_vs_returning_user": "new",
                "cohort_users": 10,
                "retained_users": 4,
                "retention_rate": 0.5,
                "data_date": "2026-03-19",
                "published_at": "2026-03-20T08:00:00Z",
            }
        ]

        errors = validate_batch_retention_daily_rows(
            rows,
            expected_data_date="2026-03-19",
            min_row_count=1,
        )
        self.assertEqual(
            errors,
            [
                "retention_rate mismatch for key="
                "2026-03-18|1|sports|US|new: expected=0.4, actual=0.5"
            ],
        )

    def test_validate_batch_retention_daily_rows_enforces_min_count(self) -> None:
        errors = validate_batch_retention_daily_rows(
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
