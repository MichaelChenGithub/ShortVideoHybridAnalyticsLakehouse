"""SQL builders for user CDC raw bronze ingestion job."""

from __future__ import annotations

from typing import Iterable, List, Tuple

try:
    from spark.rt_user_cdc_raw_contract import INVALID_CDC_USERS_TABLE, RAW_CDC_USERS_TABLE
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_user_cdc_raw_contract import INVALID_CDC_USERS_TABLE, RAW_CDC_USERS_TABLE

_REQUIRED_INVALID_CDC_USERS_COLUMNS: Tuple[Tuple[str, str], ...] = (
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

_REQUIRED_RAW_CDC_USERS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("op", "STRING"),
    ("ts_ms", "BIGINT"),
    ("schema_version", "STRING"),
    ("user_id", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("region", "STRING"),
    ("source_topic", "STRING"),
    ("source_partition", "INT"),
    ("source_offset", "BIGINT"),
    ("kafka_timestamp", "TIMESTAMP"),
    ("raw_value", "STRING"),
    ("ingested_at", "TIMESTAMP"),
)


def required_invalid_events_cdc_users_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_INVALID_CDC_USERS_COLUMNS


def required_raw_cdc_users_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_RAW_CDC_USERS_COLUMNS


def create_invalid_events_cdc_users_sql(table_name: str = INVALID_CDC_USERS_TABLE) -> str:
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


def create_raw_cdc_users_sql(table_name: str = RAW_CDC_USERS_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        op STRING,
        ts_ms BIGINT,
        schema_version STRING,
        user_id STRING,
        new_vs_returning_user STRING,
        region STRING,
        source_topic STRING,
        source_partition INT,
        source_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        raw_value STRING,
        ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(ingested_at))
    """.strip()


def missing_invalid_events_cdc_users_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [
        (name, data_type)
        for name, data_type in _REQUIRED_INVALID_CDC_USERS_COLUMNS
        if name.lower() not in existing
    ]


def missing_raw_cdc_users_columns(existing_columns: Iterable[str]) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [
        (name, data_type)
        for name, data_type in _REQUIRED_RAW_CDC_USERS_COLUMNS
        if name.lower() not in existing
    ]


def manual_alter_invalid_events_cdc_users_statements(
    existing_columns: Iterable[str],
    table_name: str = INVALID_CDC_USERS_TABLE,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_invalid_events_cdc_users_columns(existing_columns)
    ]


def manual_alter_raw_cdc_users_statements(
    existing_columns: Iterable[str],
    table_name: str = RAW_CDC_USERS_TABLE,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_raw_cdc_users_columns(existing_columns)
    ]
