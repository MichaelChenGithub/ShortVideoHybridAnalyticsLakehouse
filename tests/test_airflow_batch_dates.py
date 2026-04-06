from __future__ import annotations

import sys
import unittest
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestration.airflow_batch_dates import (  # noqa: E402
    ET_TIMEZONE,
    canonical_data_date,
    canonical_data_date_from_iso_logical_date,
    canonical_data_date_iso,
    date_range,
)


class AirflowBatchDateTests(unittest.TestCase):
    def test_canonical_data_date_uses_et_business_day_minus_one(self) -> None:
        logical_date = datetime(2026, 3, 21, 12, 0, tzinfo=ZoneInfo("UTC"))

        self.assertEqual(canonical_data_date_iso(logical_date), "2026-03-20")

    def test_canonical_data_date_uses_et_not_utc_day_boundary(self) -> None:
        logical_date = datetime(2026, 3, 21, 1, 30, tzinfo=ZoneInfo("UTC"))

        self.assertEqual(canonical_data_date_iso(logical_date), "2026-03-19")

    def test_canonical_data_date_handles_dst_transition_days(self) -> None:
        logical_date = datetime(2026, 3, 8, 8, 0, tzinfo=ZoneInfo("UTC"))

        self.assertEqual(canonical_data_date(logical_date).isoformat(), "2026-03-07")

    def test_canonical_data_date_from_iso_logical_date_normalizes_z_suffix(self) -> None:
        self.assertEqual(canonical_data_date_from_iso_logical_date("2026-03-21T08:00:00Z"), "2026-03-20")

    def test_canonical_data_date_requires_timezone_aware_input(self) -> None:
        with self.assertRaises(ValueError):
            canonical_data_date(datetime(2026, 3, 21, 8, 0))

    def test_timezone_constant_matches_contract(self) -> None:
        self.assertEqual(ET_TIMEZONE, "America/New_York")


class TestDateRange(unittest.TestCase):
    def test_returns_ordered_list_of_dates(self) -> None:
        result = date_range("2026-03-01", "2026-03-03")
        self.assertEqual(result, ["2026-03-01", "2026-03-02", "2026-03-03"])

    def test_single_date_returns_list_of_one(self) -> None:
        result = date_range("2026-03-01", "2026-03-01")
        self.assertEqual(result, ["2026-03-01"])

    def test_start_after_end_raises_value_error(self) -> None:
        with self.assertRaises(ValueError) as cm:
            date_range("2026-03-05", "2026-03-01")
        self.assertIn("start_date", str(cm.exception))

    def test_future_end_date_raises_value_error(self) -> None:
        future = (date.today() + timedelta(days=1)).isoformat()
        with self.assertRaises(ValueError) as cm:
            date_range("2026-03-01", future)
        self.assertIn("end_date", str(cm.exception))

    def test_today_end_date_raises_value_error(self) -> None:
        today = date.today().isoformat()
        with self.assertRaises(ValueError) as cm:
            date_range("2026-03-01", today)
        self.assertIn("end_date", str(cm.exception))

    def test_range_over_90_days_raises_value_error(self) -> None:
        start = date(2026, 1, 1)
        end = start + timedelta(days=90)  # 91 dates total
        with self.assertRaises(ValueError) as cm:
            date_range(start.isoformat(), end.isoformat())
        self.assertIn("90 days", str(cm.exception))

    def test_exactly_90_dates_is_allowed(self) -> None:
        start = date(2026, 1, 1)
        end = start + timedelta(days=89)  # 90 dates total
        result = date_range(start.isoformat(), end.isoformat())
        self.assertEqual(len(result), 90)
        self.assertEqual(result[0], "2026-01-01")
        self.assertEqual(result[-1], end.isoformat())


if __name__ == "__main__":
    unittest.main()
