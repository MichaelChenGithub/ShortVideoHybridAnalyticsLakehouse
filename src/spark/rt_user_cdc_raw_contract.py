"""Contract constants and runtime settings for user CDC raw bronze ingestion."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

JOB_NAME = "spark_rt_user_cdc_raw"
TOPIC = "cdc.users.profiles"
STARTING_OFFSETS = "latest"
TRIGGER_INTERVAL = "1 minute"
CHECKPOINT_RAW_CDC_USERS = "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/raw_cdc_users/v1"
CHECKPOINT_INVALID_CDC_USERS = (
    "s3a://checkpoints/jobs/spark_rt_user_cdc_raw/invalid_events_cdc_users/v1"
)

RAW_CDC_USERS_TABLE = "lakehouse.bronze.raw_cdc_users"
INVALID_CDC_USERS_TABLE = "lakehouse.bronze.invalid_events_cdc_users"
DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_CONSUMER_GROUP = "cg_rt_user_cdc_raw_v1"

ENV_BOOTSTRAP_SERVERS = "RT_USER_CDC_BOOTSTRAP_SERVERS"
ENV_TOPIC = "RT_USER_CDC_TOPIC"
ENV_STARTING_OFFSETS = "RT_USER_CDC_STARTING_OFFSETS"
ENV_TRIGGER_INTERVAL = "RT_USER_CDC_TRIGGER_INTERVAL"
ENV_CHECKPOINT_RAW_CDC_USERS = "RT_USER_CDC_CHECKPOINT_RAW"
ENV_CHECKPOINT_INVALID_CDC_USERS = "RT_USER_CDC_CHECKPOINT_INVALID"
ENV_APP_NAME = "RT_USER_CDC_APP_NAME"
ENV_CONSUMER_GROUP = "RT_USER_CDC_CONSUMER_GROUP"
ENV_RAW_CDC_USERS_TABLE = "RT_USER_CDC_RAW_TABLE"
ENV_INVALID_CDC_USERS_TABLE = "RT_USER_CDC_INVALID_TABLE"


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
    checkpoint_raw_cdc_users: str
    checkpoint_invalid_cdc_users: str
    consumer_group: str
    raw_cdc_users_table: str
    invalid_cdc_users_table: str
    use_msk_iam: bool = False


ENV_MSK_IAM_AUTH = "RT_USER_CDC_MSK_IAM_AUTH"


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, JOB_NAME),
        bootstrap_servers=values.get(ENV_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS),
        topic=values.get(ENV_TOPIC, TOPIC),
        starting_offsets=values.get(ENV_STARTING_OFFSETS, STARTING_OFFSETS),
        trigger_interval=values.get(ENV_TRIGGER_INTERVAL, TRIGGER_INTERVAL),
        checkpoint_raw_cdc_users=values.get(
            ENV_CHECKPOINT_RAW_CDC_USERS,
            CHECKPOINT_RAW_CDC_USERS,
        ),
        checkpoint_invalid_cdc_users=values.get(
            ENV_CHECKPOINT_INVALID_CDC_USERS,
            CHECKPOINT_INVALID_CDC_USERS,
        ),
        consumer_group=values.get(ENV_CONSUMER_GROUP, DEFAULT_CONSUMER_GROUP),
        raw_cdc_users_table=values.get(ENV_RAW_CDC_USERS_TABLE, RAW_CDC_USERS_TABLE),
        invalid_cdc_users_table=values.get(ENV_INVALID_CDC_USERS_TABLE, INVALID_CDC_USERS_TABLE),
        use_msk_iam=values.get(ENV_MSK_IAM_AUTH, "").lower() in ("1", "true", "yes"),
    )
