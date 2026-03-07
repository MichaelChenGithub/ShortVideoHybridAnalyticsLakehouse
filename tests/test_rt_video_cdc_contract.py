from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_video_cdc_contract import (  # noqa: E402
    CHECKPOINT_DIM_VIDEOS,
    CHECKPOINT_INVALID_CDC_VIDEOS,
    JOB_NAME,
    STARTING_OFFSETS,
    TOPIC,
    TRIGGER_INTERVAL,
    checkpoint_for_sink,
)


class RtVideoCdcContractTests(unittest.TestCase):
    def test_contract_constants_match_mic37(self) -> None:
        self.assertEqual(JOB_NAME, "spark_rt_video_cdc_upsert")
        self.assertEqual(TOPIC, "cdc.content.videos")
        self.assertEqual(STARTING_OFFSETS, "latest")
        self.assertEqual(TRIGGER_INTERVAL, "1 minute")
        self.assertEqual(
            CHECKPOINT_DIM_VIDEOS,
            "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1",
        )
        self.assertEqual(
            CHECKPOINT_INVALID_CDC_VIDEOS,
            "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1",
        )

    def test_checkpoint_builder_uses_job_scoped_path(self) -> None:
        self.assertEqual(
            checkpoint_for_sink("dim_videos"),
            "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1",
        )
        self.assertEqual(
            checkpoint_for_sink("dim_videos", version="v2"),
            "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v2",
        )
        self.assertEqual(
            checkpoint_for_sink("invalid_events_cdc_videos"),
            "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1",
        )

    def test_checkpoint_builder_rejects_blank_sink(self) -> None:
        with self.assertRaises(ValueError):
            checkpoint_for_sink("  ")


if __name__ == "__main__":
    unittest.main()
