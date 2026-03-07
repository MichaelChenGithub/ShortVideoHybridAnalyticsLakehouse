"""Contract constants and runtime settings for MIC-37 video CDC upsert."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

JOB_NAME = "spark_rt_video_cdc_upsert"
TOPIC = "cdc.content.videos"
STARTING_OFFSETS = "latest"
TRIGGER_INTERVAL = "1 minute"
CHECKPOINT_DIM_VIDEOS = "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1"
CHECKPOINT_INVALID_CDC_VIDEOS = (
    "s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1"
)

DIM_VIDEOS_TABLE = "lakehouse.dims.dim_videos"
INVALID_CDC_TABLE = "lakehouse.bronze.invalid_events_cdc_videos"
DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_CONSUMER_GROUP = "cg_rt_video_cdc_upsert_v1"

ENV_BOOTSTRAP_SERVERS = "RT_VIDEO_CDC_BOOTSTRAP_SERVERS"
ENV_TOPIC = "RT_VIDEO_CDC_TOPIC"
ENV_STARTING_OFFSETS = "RT_VIDEO_CDC_STARTING_OFFSETS"
ENV_TRIGGER_INTERVAL = "RT_VIDEO_CDC_TRIGGER_INTERVAL"
ENV_CHECKPOINT_DIM_VIDEOS = "RT_VIDEO_CDC_CHECKPOINT_DIM_VIDEOS"
ENV_CHECKPOINT_INVALID_CDC_VIDEOS = "RT_VIDEO_CDC_CHECKPOINT_INVALID_CDC_VIDEOS"
ENV_APP_NAME = "RT_VIDEO_CDC_APP_NAME"
ENV_CONSUMER_GROUP = "RT_VIDEO_CDC_CONSUMER_GROUP"
ENV_DIM_VIDEOS_TABLE = "RT_VIDEO_CDC_DIM_VIDEOS_TABLE"
ENV_INVALID_CDC_TABLE = "RT_VIDEO_CDC_INVALID_CDC_TABLE"


def checkpoint_for_sink(sink_name: str, version: str = "v1") -> str:
    sink = sink_name.strip()
    if not sink:
        raise ValueError("sink_name must be non-empty")
    return f"s3a://checkpoints/jobs/{JOB_NAME}/{sink}/{version}"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    bootstrap_servers: str
    topic: str
    starting_offsets: str
    trigger_interval: str
    checkpoint_dim_videos: str
    checkpoint_invalid_cdc: str
    consumer_group: str
    dim_videos_table: str
    invalid_cdc_table: str


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, JOB_NAME),
        bootstrap_servers=values.get(ENV_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS),
        topic=values.get(ENV_TOPIC, TOPIC),
        starting_offsets=values.get(ENV_STARTING_OFFSETS, STARTING_OFFSETS),
        trigger_interval=values.get(ENV_TRIGGER_INTERVAL, TRIGGER_INTERVAL),
        checkpoint_dim_videos=values.get(ENV_CHECKPOINT_DIM_VIDEOS, CHECKPOINT_DIM_VIDEOS),
        checkpoint_invalid_cdc=values.get(
            ENV_CHECKPOINT_INVALID_CDC_VIDEOS,
            CHECKPOINT_INVALID_CDC_VIDEOS,
        ),
        consumer_group=values.get(ENV_CONSUMER_GROUP, DEFAULT_CONSUMER_GROUP),
        dim_videos_table=values.get(ENV_DIM_VIDEOS_TABLE, DIM_VIDEOS_TABLE),
        invalid_cdc_table=values.get(ENV_INVALID_CDC_TABLE, INVALID_CDC_TABLE),
    )
