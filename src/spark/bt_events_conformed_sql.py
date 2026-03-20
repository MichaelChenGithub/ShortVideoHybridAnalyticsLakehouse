"""SQL builders for batch events_conformed contract."""

from __future__ import annotations

from typing import Tuple

EVENTS_CONFORMED_TABLE = "lakehouse.silver.events_conformed"
RAW_EVENTS_TABLE = "lakehouse.bronze.raw_events"
ET_TIMEZONE = "America/New_York"

_ALLOWED_EVENT_TYPES: Tuple[str, ...] = (
    "impression",
    "play_start",
    "play_finish",
    "like",
    "share",
    "skip",
)

_REQUIRED_EVENTS_CONFORMED_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("event_id", "STRING"),
    ("event_timestamp", "TIMESTAMP"),
    ("event_date_et", "DATE"),
    ("data_date", "DATE"),
    ("video_id", "STRING"),
    ("user_id", "STRING"),
    ("event_type", "STRING"),
    ("category", "STRING"),
    ("region", "STRING"),
)


def required_events_conformed_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_EVENTS_CONFORMED_COLUMNS


def create_events_conformed_sql(table_name: str = EVENTS_CONFORMED_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_id STRING,
        event_timestamp TIMESTAMP,
        event_date_et DATE,
        data_date DATE,
        video_id STRING,
        user_id STRING,
        event_type STRING,
        category STRING,
        region STRING
    ) USING iceberg
    PARTITIONED BY (event_date_et, bucket(64, user_id))
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def default_d_minus_1_data_date_sql() -> str:
    return f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)"


def delete_events_conformed_data_date_sql(
    *,
    target_table: str = EVENTS_CONFORMED_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    DELETE FROM {target_table}
    WHERE data_date = {target_data_date}
    """.strip()


def insert_events_conformed_for_data_date_sql(
    *,
    source_table: str = RAW_EVENTS_TABLE,
    target_table: str = EVENTS_CONFORMED_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    allowed_event_types_sql = ", ".join(f"'{event_type}'" for event_type in _ALLOWED_EVENT_TYPES)

    return f"""
    INSERT INTO {target_table}
    WITH source_filtered AS (
        SELECT
            event_id,
            event_timestamp,
            video_id,
            user_id,
            LOWER(TRIM(event_type)) AS event_type,
            payload_json,
            source_partition,
            source_offset,
            ingested_at,
            to_date(from_utc_timestamp(event_timestamp, '{ET_TIMEZONE}')) AS event_date_et
        FROM {source_table}
        WHERE event_id IS NOT NULL
          AND event_timestamp IS NOT NULL
          AND video_id IS NOT NULL
          AND user_id IS NOT NULL
          AND event_type IS NOT NULL
          AND LOWER(TRIM(event_type)) IN ({allowed_event_types_sql})
    ),
    date_slice AS (
        SELECT
            event_id,
            event_timestamp,
            video_id,
            user_id,
            event_type,
            payload_json,
            source_partition,
            source_offset,
            ingested_at,
            event_date_et
        FROM source_filtered
        WHERE event_date_et = {target_data_date}
    ),
    deduped AS (
        SELECT
            event_id,
            event_timestamp,
            video_id,
            user_id,
            event_type,
            payload_json,
            event_date_et
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY source_partition DESC NULLS LAST,
                             source_offset DESC NULLS LAST,
                             ingested_at DESC NULLS LAST
                ) AS rn
            FROM date_slice
        ) ranked
        WHERE rn = 1
    )
    SELECT
        event_id,
        event_timestamp,
        event_date_et,
        event_date_et AS data_date,
        video_id,
        user_id,
        event_type,
        COALESCE(NULLIF(TRIM(get_json_object(payload_json, '$.category')), ''), 'unknown') AS category,
        COALESCE(NULLIF(TRIM(get_json_object(payload_json, '$.region')), ''), 'unknown') AS region
    FROM deduped
    """.strip()
