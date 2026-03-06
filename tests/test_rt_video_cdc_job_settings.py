from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_video_cdc_contract import (  # noqa: E402
    CHECKPOINT_DIM_VIDEOS,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CONSUMER_GROUP,
    DIM_VIDEOS_TABLE,
    ENV_APP_NAME,
    ENV_BOOTSTRAP_SERVERS,
    ENV_CHECKPOINT_DIM_VIDEOS,
    ENV_CONSUMER_GROUP,
    ENV_DIM_VIDEOS_TABLE,
    ENV_STARTING_OFFSETS,
    ENV_TOPIC,
    ENV_TRIGGER_INTERVAL,
    JOB_NAME,
    STARTING_OFFSETS,
    TOPIC,
    TRIGGER_INTERVAL,
    load_job_settings,
)


class RtVideoCdcJobSettingsTests(unittest.TestCase):
    def test_load_job_settings_defaults(self) -> None:
        settings = load_job_settings({})
        self.assertEqual(settings.app_name, JOB_NAME)
        self.assertEqual(settings.bootstrap_servers, DEFAULT_BOOTSTRAP_SERVERS)
        self.assertEqual(settings.topic, TOPIC)
        self.assertEqual(settings.starting_offsets, STARTING_OFFSETS)
        self.assertEqual(settings.trigger_interval, TRIGGER_INTERVAL)
        self.assertEqual(settings.checkpoint_dim_videos, CHECKPOINT_DIM_VIDEOS)
        self.assertEqual(settings.consumer_group, DEFAULT_CONSUMER_GROUP)
        self.assertEqual(settings.dim_videos_table, DIM_VIDEOS_TABLE)

    def test_load_job_settings_honors_env_overrides(self) -> None:
        settings = load_job_settings(
            {
                ENV_APP_NAME: "custom-app",
                ENV_BOOTSTRAP_SERVERS: "localhost:9092",
                ENV_TOPIC: "cdc.custom.videos",
                ENV_STARTING_OFFSETS: "earliest",
                ENV_TRIGGER_INTERVAL: "30 seconds",
                ENV_CHECKPOINT_DIM_VIDEOS: "s3a://checkpoints/custom/path/v1",
                ENV_CONSUMER_GROUP: "cg_custom",
                ENV_DIM_VIDEOS_TABLE: "lakehouse.dims.custom_dim_videos",
            }
        )
        self.assertEqual(settings.app_name, "custom-app")
        self.assertEqual(settings.bootstrap_servers, "localhost:9092")
        self.assertEqual(settings.topic, "cdc.custom.videos")
        self.assertEqual(settings.starting_offsets, "earliest")
        self.assertEqual(settings.trigger_interval, "30 seconds")
        self.assertEqual(settings.checkpoint_dim_videos, "s3a://checkpoints/custom/path/v1")
        self.assertEqual(settings.consumer_group, "cg_custom")
        self.assertEqual(settings.dim_videos_table, "lakehouse.dims.custom_dim_videos")


if __name__ == "__main__":
    unittest.main()
