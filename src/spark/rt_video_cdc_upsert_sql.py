"""SQL builders for MIC-37 video CDC upsert job."""

from __future__ import annotations

from typing import Iterable, List, Tuple

try:
    from spark.rt_video_cdc_contract import DIM_VIDEOS_TABLE, INVALID_CDC_TABLE
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_video_cdc_contract import DIM_VIDEOS_TABLE, INVALID_CDC_TABLE

_REQUIRED_DIM_VIDEOS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("video_id", "STRING"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("upload_time", "TIMESTAMP"),
    ("status", "STRING"),
    ("updated_at", "TIMESTAMP"),
    ("source_ts_ms", "BIGINT"),
)

_REQUIRED_INVALID_CDC_COLUMNS: Tuple[Tuple[str, str], ...] = (
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


def required_dim_videos_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_DIM_VIDEOS_COLUMNS


def required_invalid_events_cdc_videos_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_INVALID_CDC_COLUMNS


def create_dim_videos_sql(table_name: str = DIM_VIDEOS_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        video_id STRING,
        category STRING,
        region STRING,
        upload_time TIMESTAMP,
        status STRING,
        updated_at TIMESTAMP,
        source_ts_ms BIGINT
    ) USING iceberg
    PARTITIONED BY (bucket(16, video_id))
    TBLPROPERTIES (
        'write.merge.mode'='merge-on-read',
        'format-version'='2'
    )
    """.strip()


def create_invalid_events_cdc_videos_sql(table_name: str = INVALID_CDC_TABLE) -> str:
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
    PARTITIONED BY (days(ingested_at))
    """.strip()


def missing_required_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [(name, data_type) for name, data_type in _REQUIRED_DIM_VIDEOS_COLUMNS if name.lower() not in existing]


def manual_alter_statements(
    existing_columns: Iterable[str],
    table_name: str = DIM_VIDEOS_TABLE,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_required_columns(existing_columns)
    ]


def missing_invalid_events_cdc_videos_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [
        (name, data_type)
        for name, data_type in _REQUIRED_INVALID_CDC_COLUMNS
        if name.lower() not in existing
    ]


def manual_alter_invalid_events_cdc_videos_statements(
    existing_columns: Iterable[str],
    table_name: str = INVALID_CDC_TABLE,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_invalid_events_cdc_videos_columns(existing_columns)
    ]


def merge_dim_videos_sql(
    source_view: str = "video_updates",
    table_name: str = DIM_VIDEOS_TABLE,
) -> str:
    return f"""
    MERGE INTO {table_name} AS target
    USING (
        SELECT
            video_id,
            category,
            region,
            CAST(upload_time AS TIMESTAMP) AS upload_time,
            status,
            ts_ms
        FROM (
            SELECT
                video_id,
                category,
                region,
                upload_time,
                status,
                ts_ms,
                source_offset,
                ROW_NUMBER() OVER (
                    PARTITION BY video_id
                    ORDER BY ts_ms DESC, source_offset DESC
                ) AS rn
            FROM {source_view}
            WHERE op IN ('c', 'u')
              AND video_id IS NOT NULL
              AND ts_ms IS NOT NULL
        ) ranked
        WHERE rn = 1
    ) AS source
    ON target.video_id = source.video_id
    WHEN MATCHED THEN UPDATE SET
        target.category = source.category,
        target.region = source.region,
        target.upload_time = source.upload_time,
        target.status = source.status,
        target.updated_at = current_timestamp(),
        target.source_ts_ms = source.ts_ms
    WHEN NOT MATCHED THEN INSERT (
        video_id,
        category,
        region,
        upload_time,
        status,
        updated_at,
        source_ts_ms
    ) VALUES (
        source.video_id,
        source.category,
        source.region,
        source.upload_time,
        source.status,
        current_timestamp(),
        source.ts_ms
    )
    """.strip()
