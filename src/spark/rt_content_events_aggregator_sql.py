"""SQL builders and schema contract helpers for MIC-39 content aggregator."""

from __future__ import annotations

from typing import Iterable, List, Tuple

try:
    from spark.rt_content_events_contract import (
        INVALID_EVENTS_CONTENT_TABLE,
        RAW_EVENTS_TABLE,
        RT_VIDEO_STATS_1MIN_TABLE,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_content_events_contract import (
        INVALID_EVENTS_CONTENT_TABLE,
        RAW_EVENTS_TABLE,
        RT_VIDEO_STATS_1MIN_TABLE,
    )

_REQUIRED_RAW_EVENTS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("event_id", "STRING"),
    ("event_timestamp", "TIMESTAMP"),
    ("video_id", "STRING"),
    ("user_id", "STRING"),
    ("event_type", "STRING"),
    ("payload_json", "STRING"),
    ("schema_version", "STRING"),
    ("source_topic", "STRING"),
    ("source_partition", "INT"),
    ("source_offset", "BIGINT"),
    ("ingested_at", "TIMESTAMP"),
)

_REQUIRED_RT_VIDEO_STATS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("video_id", "STRING"),
    ("window_start", "TIMESTAMP"),
    ("window_end", "TIMESTAMP"),
    ("impressions", "BIGINT"),
    ("play_start", "BIGINT"),
    ("play_finish", "BIGINT"),
    ("likes", "BIGINT"),
    ("shares", "BIGINT"),
    ("skips", "BIGINT"),
    ("watch_time_sum_ms", "BIGINT"),
    ("processed_at", "TIMESTAMP"),
)

_REQUIRED_INVALID_EVENTS_CONTENT_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("invalid_event_id", "STRING"),
    ("raw_value", "STRING"),
    ("source_topic", "STRING"),
    ("source_partition", "INT"),
    ("source_offset", "BIGINT"),
    ("schema_version", "STRING"),
    ("error_code", "STRING"),
    ("error_reason", "STRING"),
    ("ingested_at", "TIMESTAMP"),
)


def required_raw_events_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_RAW_EVENTS_COLUMNS


def required_rt_video_stats_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_RT_VIDEO_STATS_COLUMNS


def required_invalid_events_content_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_INVALID_EVENTS_CONTENT_COLUMNS


def create_raw_events_sql(table_name: str = RAW_EVENTS_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_id STRING,
        event_timestamp TIMESTAMP,
        video_id STRING,
        user_id STRING,
        event_type STRING,
        payload_json STRING,
        schema_version STRING,
        source_topic STRING,
        source_partition INT,
        source_offset BIGINT,
        ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(event_timestamp))
    """.strip()


def create_rt_video_stats_sql(table_name: str = RT_VIDEO_STATS_1MIN_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        video_id STRING,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        impressions BIGINT,
        play_start BIGINT,
        play_finish BIGINT,
        likes BIGINT,
        shares BIGINT,
        skips BIGINT,
        watch_time_sum_ms BIGINT,
        processed_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(window_start), bucket(16, video_id))
    """.strip()


def create_invalid_events_content_sql(table_name: str = INVALID_EVENTS_CONTENT_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        invalid_event_id STRING,
        raw_value STRING,
        source_topic STRING,
        source_partition INT,
        source_offset BIGINT,
        schema_version STRING,
        error_code STRING,
        error_reason STRING,
        ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(ingested_at), bucket(16, source_topic))
    """.strip()


def merge_rt_video_stats_sql(
    source_view: str = "gold_updates",
    table_name: str = RT_VIDEO_STATS_1MIN_TABLE,
) -> str:
    return f"""
    MERGE INTO {table_name} AS target
    USING {source_view} AS source
    ON target.video_id = source.video_id
       AND target.window_start = source.window_start
    WHEN MATCHED THEN UPDATE SET
        target.window_end = source.window_end,
        target.impressions = source.impressions,
        target.play_start = source.play_start,
        target.play_finish = source.play_finish,
        target.likes = source.likes,
        target.shares = source.shares,
        target.skips = source.skips,
        target.watch_time_sum_ms = source.watch_time_sum_ms,
        target.processed_at = source.processed_at
    WHEN NOT MATCHED THEN INSERT (
        video_id,
        window_start,
        window_end,
        impressions,
        play_start,
        play_finish,
        likes,
        shares,
        skips,
        watch_time_sum_ms,
        processed_at
    ) VALUES (
        source.video_id,
        source.window_start,
        source.window_end,
        source.impressions,
        source.play_start,
        source.play_finish,
        source.likes,
        source.shares,
        source.skips,
        source.watch_time_sum_ms,
        source.processed_at
    )
    """.strip()


def missing_required_columns(
    existing_columns: Iterable[str],
    required_columns: Iterable[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [
        (name, data_type)
        for name, data_type in required_columns
        if name.lower() not in existing
    ]


def missing_raw_events_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    return missing_required_columns(existing_columns, _REQUIRED_RAW_EVENTS_COLUMNS)


def missing_rt_video_stats_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    return missing_required_columns(existing_columns, _REQUIRED_RT_VIDEO_STATS_COLUMNS)


def missing_invalid_events_content_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    return missing_required_columns(existing_columns, _REQUIRED_INVALID_EVENTS_CONTENT_COLUMNS)


def manual_alter_statements(
    existing_columns: Iterable[str],
    required_columns: Iterable[Tuple[str, str]],
    table_name: str,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_required_columns(existing_columns, required_columns)
    ]


def manual_alter_raw_events_statements(
    existing_columns: Iterable[str],
    table_name: str = RAW_EVENTS_TABLE,
) -> List[str]:
    return manual_alter_statements(existing_columns, _REQUIRED_RAW_EVENTS_COLUMNS, table_name)


def manual_alter_rt_video_stats_statements(
    existing_columns: Iterable[str],
    table_name: str = RT_VIDEO_STATS_1MIN_TABLE,
) -> List[str]:
    return manual_alter_statements(existing_columns, _REQUIRED_RT_VIDEO_STATS_COLUMNS, table_name)


def manual_alter_invalid_events_content_statements(
    existing_columns: Iterable[str],
    table_name: str = INVALID_EVENTS_CONTENT_TABLE,
) -> List[str]:
    return manual_alter_statements(existing_columns, _REQUIRED_INVALID_EVENTS_CONTENT_COLUMNS, table_name)
