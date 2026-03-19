from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_rt_user_cdc_raw_bronze import validate_raw_cdc_rows  # noqa: E402


class VerifyRtUserCdcRawBronzeTests(unittest.TestCase):
    def test_validation_passes_for_expected_latest_row(self) -> None:
        rows = [
            {
                "op": "c",
                "ts_ms": 1000,
                "schema_version": "m2_v1",
                "user_id": "user_001",
                "new_vs_returning_user": "new",
                "region": "NA",
                "source_topic": "cdc.users.profiles",
                "source_partition": 0,
                "source_offset": 10,
                "kafka_timestamp": "2026-03-16T00:00:00Z",
                "raw_value": '{"op":"c"}',
                "ingested_at": "2026-03-16T00:00:01Z",
            },
            {
                "op": "u",
                "ts_ms": 2000,
                "schema_version": "m2_v1",
                "user_id": "user_001",
                "new_vs_returning_user": "returning",
                "region": "LATAM",
                "source_topic": "cdc.users.profiles",
                "source_partition": 0,
                "source_offset": 11,
                "kafka_timestamp": "2026-03-16T00:00:02Z",
                "raw_value": '{"op":"u"}',
                "ingested_at": "2026-03-16T00:00:03Z",
            },
        ]

        errors = validate_raw_cdc_rows(
            rows,
            user_id="user_001",
            min_row_count=2,
            expected_state="returning",
            expected_region="LATAM",
            expected_latest_ts_ms=2000,
            min_source_ts_ms=1000,
        )

        self.assertEqual(errors, [])

    def test_validation_fails_for_low_count(self) -> None:
        rows = [
            {
                "op": "u",
                "ts_ms": 2000,
                "schema_version": None,
                "user_id": "user_002",
                "new_vs_returning_user": None,
                "region": None,
                "source_topic": None,
                "source_partition": 0,
                "source_offset": 11,
                "kafka_timestamp": None,
                "raw_value": None,
                "ingested_at": "2026-03-16T00:00:03Z",
            }
        ]

        errors = validate_raw_cdc_rows(
            rows,
            user_id="user_002",
            min_row_count=2,
            expected_state="returning",
            expected_region="LATAM",
            expected_latest_ts_ms=3000,
        )

        self.assertEqual(
            errors,
            [
                "raw_cdc_users row count below threshold for user_id=user_002: row_count=1, min_row_count=2"
            ],
        )

    def test_validation_fails_for_latest_state_region_ts_and_op_mismatch(self) -> None:
        rows = [
            {
                "op": "c",
                "ts_ms": 1000,
                "schema_version": "m2_v1",
                "user_id": "user_003",
                "new_vs_returning_user": "new",
                "region": "NA",
                "source_topic": "cdc.users.profiles",
                "source_partition": 0,
                "source_offset": 10,
                "kafka_timestamp": "2026-03-16T00:00:00Z",
                "raw_value": '{"op":"c"}',
                "ingested_at": "2026-03-16T00:00:01Z",
            },
            {
                "op": "d",
                "ts_ms": 2000,
                "schema_version": "m2_v1",
                "user_id": "user_003",
                "new_vs_returning_user": "new",
                "region": "NA",
                "source_topic": "cdc.users.profiles",
                "source_partition": 0,
                "source_offset": 11,
                "kafka_timestamp": "2026-03-16T00:00:02Z",
                "raw_value": '{"op":"d"}',
                "ingested_at": "2026-03-16T00:00:03Z",
            },
        ]

        errors = validate_raw_cdc_rows(
            rows,
            user_id="user_003",
            min_row_count=2,
            expected_state="returning",
            expected_region="LATAM",
            expected_latest_ts_ms=3000,
        )

        joined = "\n".join(errors)
        self.assertIn("latest raw user CDC state mismatch", joined)
        self.assertIn("latest raw user CDC region mismatch", joined)
        self.assertIn("latest raw user CDC ts_ms mismatch", joined)
        self.assertIn("latest raw user CDC op must be c/u", joined)


if __name__ == "__main__":
    unittest.main()
