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
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CONSUMER_GROUP,
    ENV_APP_NAME,
    ENV_BOOTSTRAP_SERVERS,
    ENV_CHECKPOINT_GOLD,
    ENV_CHECKPOINT_INVALID,
    ENV_CHECKPOINT_RAW,
    ENV_CONSUMER_GROUP,
    ENV_GOLD_TABLE,
    ENV_INVALID_TABLE,
    ENV_RAW_TABLE,
    ENV_STARTING_OFFSETS,
    ENV_TOPIC,
    ENV_TRIGGER_GOLD,
    ENV_TRIGGER_RAW,
    ENV_WATERMARK_GOLD,
    INVALID_EVENTS_CONTENT_TABLE,
    JOB_NAME,
    RAW_EVENTS_TABLE,
    RT_VIDEO_STATS_1MIN_TABLE,
    STARTING_OFFSETS,
    TOPIC,
    TRIGGER_GOLD,
    TRIGGER_RAW,
    WATERMARK_GOLD,
    load_job_settings,
)


class RtContentEventsJobSettingsTests(unittest.TestCase):
    def test_load_job_settings_defaults(self) -> None:
        settings = load_job_settings({})
        self.assertEqual(settings.app_name, JOB_NAME)
        self.assertEqual(settings.bootstrap_servers, DEFAULT_BOOTSTRAP_SERVERS)
        self.assertEqual(settings.topic, TOPIC)
        self.assertEqual(settings.starting_offsets, STARTING_OFFSETS)
        self.assertEqual(settings.trigger_raw, TRIGGER_RAW)
        self.assertEqual(settings.trigger_gold, TRIGGER_GOLD)
        self.assertEqual(settings.watermark_gold, WATERMARK_GOLD)
        self.assertEqual(settings.checkpoint_raw, CHECKPOINT_RAW)
        self.assertEqual(settings.checkpoint_gold, CHECKPOINT_GOLD)
        self.assertEqual(settings.checkpoint_invalid, CHECKPOINT_INVALID)
        self.assertEqual(settings.consumer_group, DEFAULT_CONSUMER_GROUP)
        self.assertEqual(settings.raw_table, RAW_EVENTS_TABLE)
        self.assertEqual(settings.gold_table, RT_VIDEO_STATS_1MIN_TABLE)
        self.assertEqual(settings.invalid_table, INVALID_EVENTS_CONTENT_TABLE)

    def test_load_job_settings_honors_env_overrides(self) -> None:
        settings = load_job_settings(
            {
                ENV_APP_NAME: "custom-content-app",
                ENV_BOOTSTRAP_SERVERS: "localhost:9092",
                ENV_TOPIC: "content.events.custom",
                ENV_STARTING_OFFSETS: "earliest",
                ENV_TRIGGER_RAW: "30 seconds",
                ENV_TRIGGER_GOLD: "2 minutes",
                ENV_WATERMARK_GOLD: "5 minutes",
                ENV_CHECKPOINT_RAW: "s3a://checkpoints/custom/raw/v1",
                ENV_CHECKPOINT_GOLD: "s3a://checkpoints/custom/gold/v1",
                ENV_CHECKPOINT_INVALID: "s3a://checkpoints/custom/invalid/v1",
                ENV_CONSUMER_GROUP: "cg_custom_content",
                ENV_RAW_TABLE: "lakehouse.bronze.raw_events_custom",
                ENV_GOLD_TABLE: "lakehouse.gold.rt_video_stats_1min_custom",
                ENV_INVALID_TABLE: "lakehouse.bronze.invalid_events_content_custom",
            }
        )
        self.assertEqual(settings.app_name, "custom-content-app")
        self.assertEqual(settings.bootstrap_servers, "localhost:9092")
        self.assertEqual(settings.topic, "content.events.custom")
        self.assertEqual(settings.starting_offsets, "earliest")
        self.assertEqual(settings.trigger_raw, "30 seconds")
        self.assertEqual(settings.trigger_gold, "2 minutes")
        self.assertEqual(settings.watermark_gold, "5 minutes")
        self.assertEqual(settings.checkpoint_raw, "s3a://checkpoints/custom/raw/v1")
        self.assertEqual(settings.checkpoint_gold, "s3a://checkpoints/custom/gold/v1")
        self.assertEqual(settings.checkpoint_invalid, "s3a://checkpoints/custom/invalid/v1")
        self.assertEqual(settings.consumer_group, "cg_custom_content")
        self.assertEqual(settings.raw_table, "lakehouse.bronze.raw_events_custom")
        self.assertEqual(settings.gold_table, "lakehouse.gold.rt_video_stats_1min_custom")
        self.assertEqual(settings.invalid_table, "lakehouse.bronze.invalid_events_content_custom")


if __name__ == "__main__":
    unittest.main()
