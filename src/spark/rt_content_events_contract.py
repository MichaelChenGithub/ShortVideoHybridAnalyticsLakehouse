"""Contract constants and runtime settings for MIC-40 content events aggregation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

JOB_NAME = "spark_rt_content_events_aggregator"
TOPIC = "content_events"
STARTING_OFFSETS = "latest"
TRIGGER_RAW = "10 seconds"
TRIGGER_GOLD = "1 minute"

RAW_EVENTS_TABLE = "lakehouse.bronze.raw_events"
RT_VIDEO_STATS_1MIN_TABLE = "lakehouse.gold.rt_video_stats_1min"

CHECKPOINT_RAW = "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1"
CHECKPOINT_GOLD = "s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1"

DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_CONSUMER_GROUP = "cg_rt_content_events_aggregator_v1"

ENV_BOOTSTRAP_SERVERS = "RT_CONTENT_EVENTS_BOOTSTRAP_SERVERS"
ENV_TOPIC = "RT_CONTENT_EVENTS_TOPIC"
ENV_STARTING_OFFSETS = "RT_CONTENT_EVENTS_STARTING_OFFSETS"
ENV_TRIGGER_RAW = "RT_CONTENT_EVENTS_TRIGGER_RAW"
ENV_TRIGGER_GOLD = "RT_CONTENT_EVENTS_TRIGGER_GOLD"
ENV_CHECKPOINT_RAW = "RT_CONTENT_EVENTS_CHECKPOINT_RAW"
ENV_CHECKPOINT_GOLD = "RT_CONTENT_EVENTS_CHECKPOINT_GOLD"
ENV_APP_NAME = "RT_CONTENT_EVENTS_APP_NAME"
ENV_CONSUMER_GROUP = "RT_CONTENT_EVENTS_CONSUMER_GROUP"
ENV_RAW_TABLE = "RT_CONTENT_EVENTS_RAW_TABLE"
ENV_GOLD_TABLE = "RT_CONTENT_EVENTS_GOLD_TABLE"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    bootstrap_servers: str
    topic: str
    starting_offsets: str
    trigger_raw: str
    trigger_gold: str
    checkpoint_raw: str
    checkpoint_gold: str
    consumer_group: str
    raw_table: str
    gold_table: str


def checkpoint_for_sink(sink_name: str, version: str = "v1") -> str:
    sink = sink_name.strip()
    if not sink:
        raise ValueError("sink_name must be non-empty")
    return f"s3a://checkpoints/jobs/{JOB_NAME}/{sink}/{version}"


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, JOB_NAME),
        bootstrap_servers=values.get(ENV_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS),
        topic=values.get(ENV_TOPIC, TOPIC),
        starting_offsets=values.get(ENV_STARTING_OFFSETS, STARTING_OFFSETS),
        trigger_raw=values.get(ENV_TRIGGER_RAW, TRIGGER_RAW),
        trigger_gold=values.get(ENV_TRIGGER_GOLD, TRIGGER_GOLD),
        checkpoint_raw=values.get(ENV_CHECKPOINT_RAW, CHECKPOINT_RAW),
        checkpoint_gold=values.get(ENV_CHECKPOINT_GOLD, CHECKPOINT_GOLD),
        consumer_group=values.get(ENV_CONSUMER_GROUP, DEFAULT_CONSUMER_GROUP),
        raw_table=values.get(ENV_RAW_TABLE, RAW_EVENTS_TABLE),
        gold_table=values.get(ENV_GOLD_TABLE, RT_VIDEO_STATS_1MIN_TABLE),
    )
