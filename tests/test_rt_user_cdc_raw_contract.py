from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_user_cdc_raw_contract import (  # noqa: E402
    CHECKPOINT_INVALID_CDC_USERS,
    CHECKPOINT_RAW_CDC_USERS,
    ENV_RAW_CDC_USERS_TABLE,
    JOB_NAME,
    RAW_CDC_USERS_TABLE,
    STARTING_OFFSETS,
    TOPIC,
    TRIGGER_INTERVAL,
    checkpoint_for_sink,
)


class RtUserCdcRawContractTests(unittest.TestCase):
    def test_contract_constants_match_mic147(self) -> None:
        self.assertEqual(JOB_NAME, "spark_rt_user_cdc_raw")
        self.assertEqual(TOPIC, "cdc.users.profiles")
        self.assertEqual(STARTING_OFFSETS, "latest")
        self.assertEqual(TRIGGER_INTERVAL, "1 minute")
        self.assertEqual(
            CHECKPOINT_RAW_CDC_USERS,
            "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/raw_cdc_users/v1",
        )
        self.assertEqual(
            CHECKPOINT_INVALID_CDC_USERS,
            "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/invalid_events_cdc_users/v1",
        )
        self.assertEqual(RAW_CDC_USERS_TABLE, "lakehouse.bronze.raw_cdc_users")
        self.assertEqual(ENV_RAW_CDC_USERS_TABLE, "RT_USER_CDC_RAW_TABLE")

    def test_checkpoint_builder_uses_job_scoped_path(self) -> None:
        self.assertEqual(
            checkpoint_for_sink("raw_cdc_users"),
            "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/raw_cdc_users/v1",
        )
        self.assertEqual(
            checkpoint_for_sink("raw_cdc_users", version="v2"),
            "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/raw_cdc_users/v2",
        )
        self.assertEqual(
            checkpoint_for_sink("invalid_events_cdc_users"),
            "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/invalid_events_cdc_users/v1",
        )

    def test_checkpoint_builder_rejects_blank_sink(self) -> None:
        with self.assertRaises(ValueError):
            checkpoint_for_sink("  ")


if __name__ == "__main__":
    unittest.main()
