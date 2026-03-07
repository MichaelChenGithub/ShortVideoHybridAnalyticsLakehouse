from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_content_events_contract import (  # noqa: E402
    CHECKPOINT_GOLD,
    CHECKPOINT_INVALID,
    CHECKPOINT_RAW,
    JOB_NAME,
    STARTING_OFFSETS,
    TOPIC,
    TRIGGER_GOLD,
    TRIGGER_RAW,
    checkpoint_for_sink,
)


class RtContentEventsContractTests(unittest.TestCase):
    def test_contract_constants_match_mic39(self) -> None:
        self.assertEqual(JOB_NAME, "spark_rt_content_events_aggregator")
        self.assertEqual(TOPIC, "content_events")
        self.assertEqual(STARTING_OFFSETS, "latest")
        self.assertEqual(TRIGGER_RAW, "10 seconds")
        self.assertEqual(TRIGGER_GOLD, "1 minute")
        self.assertEqual(
            CHECKPOINT_RAW,
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1",
        )
        self.assertEqual(
            CHECKPOINT_GOLD,
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1",
        )
        self.assertEqual(
            CHECKPOINT_INVALID,
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1",
        )

    def test_checkpoint_builder_uses_job_scoped_path(self) -> None:
        self.assertEqual(
            checkpoint_for_sink("raw_events"),
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1",
        )
        self.assertEqual(
            checkpoint_for_sink("rt_video_stats_1min", version="v2"),
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v2",
        )
        self.assertEqual(
            checkpoint_for_sink("invalid_events_content"),
            "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1",
        )

    def test_checkpoint_builder_rejects_blank_sink(self) -> None:
        with self.assertRaises(ValueError):
            checkpoint_for_sink("  ")


if __name__ == "__main__":
    unittest.main()
