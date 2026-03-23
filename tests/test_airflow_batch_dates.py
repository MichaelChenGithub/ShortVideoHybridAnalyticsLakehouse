from __future__ import annotations

import sys
import unittest
from datetime import datetime
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


if __name__ == "__main__":
    unittest.main()
