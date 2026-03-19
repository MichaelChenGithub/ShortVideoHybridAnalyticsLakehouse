from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bt_dim_users_scd2 import validate_scd2_rows  # noqa: E402

BASE_TS_MS = int(datetime(2026, 3, 19, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)


class VerifyBtDimUsersScd2Tests(unittest.TestCase):
    def test_validation_passes_for_valid_intervals_and_as_of_probes(self) -> None:
        rows = [
            {
                "user_id": "u_001",
                "region": "NA",
                "new_vs_returning_user": "new",
                "valid_from": datetime(2026, 3, 19, 0, 0, 0),
                "valid_to": datetime(2026, 3, 19, 0, 0, 2),
                "is_current": False,
            },
            {
                "user_id": "u_001",
                "region": "LATAM",
                "new_vs_returning_user": "returning",
                "valid_from": datetime(2026, 3, 19, 0, 0, 2),
                "valid_to": datetime(9999, 12, 31, 0, 0, 0),
                "is_current": True,
            },
        ]

        errors = validate_scd2_rows(
            rows,
            user_id="u_001",
            expect_latest_state="returning",
            expect_latest_region="LATAM",
            probe_old_ts_ms=BASE_TS_MS + 1_000,
            expect_state_at_old="new",
            probe_new_ts_ms=BASE_TS_MS + 2_000,
            expect_state_at_new="returning",
        )

        self.assertEqual(errors, [])

    def test_validation_fails_when_state_is_not_governed(self) -> None:
        rows = [
            {
                "user_id": "u_002",
                "region": "NA",
                "new_vs_returning_user": "brand_new",
                "valid_from": datetime(2026, 3, 19, 0, 0, 0),
                "valid_to": datetime(9999, 12, 31, 0, 0, 0),
                "is_current": True,
            }
        ]

        errors = validate_scd2_rows(rows, user_id="u_002")
        joined = "\n".join(errors)
        self.assertIn("must be governed value", joined)

    def test_validation_fails_for_overlapping_intervals(self) -> None:
        rows = [
            {
                "user_id": "u_003",
                "region": "NA",
                "new_vs_returning_user": "new",
                "valid_from": datetime(2026, 3, 19, 0, 0, 0),
                "valid_to": datetime(2026, 3, 19, 0, 0, 3),
                "is_current": False,
            },
            {
                "user_id": "u_003",
                "region": "LATAM",
                "new_vs_returning_user": "returning",
                "valid_from": datetime(2026, 3, 19, 0, 0, 2),
                "valid_to": datetime(9999, 12, 31, 0, 0, 0),
                "is_current": True,
            },
        ]

        errors = validate_scd2_rows(rows, user_id="u_003")
        joined = "\n".join(errors)
        self.assertIn("Overlapping intervals detected", joined)

    def test_validation_fails_for_as_of_mismatch(self) -> None:
        rows = [
            {
                "user_id": "u_004",
                "region": "NA",
                "new_vs_returning_user": "new",
                "valid_from": datetime(2026, 3, 19, 0, 0, 0),
                "valid_to": datetime(2026, 3, 19, 0, 0, 2),
                "is_current": False,
            },
            {
                "user_id": "u_004",
                "region": "LATAM",
                "new_vs_returning_user": "returning",
                "valid_from": datetime(2026, 3, 19, 0, 0, 2),
                "valid_to": datetime(9999, 12, 31, 0, 0, 0),
                "is_current": True,
            },
        ]

        errors = validate_scd2_rows(
            rows,
            user_id="u_004",
            probe_old_ts_ms=BASE_TS_MS + 1_000,
            expect_state_at_old="returning",
        )
        joined = "\n".join(errors)
        self.assertIn("As-of old probe state mismatch", joined)


if __name__ == "__main__":
    unittest.main()
