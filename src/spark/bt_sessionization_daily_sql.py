"""SQL builders for batch sessionization_daily contract."""

from __future__ import annotations

from typing import Tuple

BATCH_SESSIONIZATION_DAILY_TABLE = "lakehouse.gold.batch_sessionization_daily"
USER_ACTIVITY_SESSIONS_30M_TABLE = "lakehouse.silver.user_activity_sessions_30m"
ET_TIMEZONE = "America/New_York"

_REQUIRED_BATCH_SESSIONIZATION_DAILY_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("data_date", "DATE"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("sessions", "BIGINT"),
    ("sessions_per_user", "DOUBLE"),
    ("avg_session_duration_sec", "DOUBLE"),
    ("events_per_session", "DOUBLE"),
    ("watch_time_per_session_ms", "DOUBLE"),
    ("published_at", "TIMESTAMP"),
)


def required_batch_sessionization_daily_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_BATCH_SESSIONIZATION_DAILY_COLUMNS


def create_batch_sessionization_daily_sql(
    table_name: str = BATCH_SESSIONIZATION_DAILY_TABLE,
) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        data_date DATE,
        category STRING,
        region STRING,
        new_vs_returning_user STRING,
        sessions BIGINT,
        sessions_per_user DOUBLE,
        avg_session_duration_sec DOUBLE,
        events_per_session DOUBLE,
        watch_time_per_session_ms DOUBLE,
        published_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (data_date)
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def default_d_minus_1_data_date_sql() -> str:
    return f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)"


def delete_batch_sessionization_daily_data_date_sql(
    *,
    target_table: str = BATCH_SESSIONIZATION_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    DELETE FROM {target_table}
    WHERE data_date = {target_data_date}
    """.strip()


def insert_batch_sessionization_daily_for_data_date_sql(
    *,
    source_table: str = USER_ACTIVITY_SESSIONS_30M_TABLE,
    target_table: str = BATCH_SESSIONIZATION_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    INSERT INTO {target_table}
    WITH sessionized AS (
        SELECT
            data_date,
            COALESCE(category, 'unknown') AS category,
            COALESCE(region, 'unknown') AS region,
            COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user,
            session_id,
            user_id,
            session_duration_sec,
            event_count,
            watch_time_sum_ms
        FROM {source_table}
        WHERE data_date = {target_data_date}
    ),
    aggregated AS (
        SELECT
            data_date,
            category,
            region,
            new_vs_returning_user,
            COUNT(session_id) AS sessions,
            COUNT(DISTINCT user_id) AS distinct_active_users,
            AVG(CAST(session_duration_sec AS DOUBLE)) AS avg_session_duration_sec,
            SUM(event_count) AS total_events,
            SUM(watch_time_sum_ms) AS total_watch_time_sum_ms
        FROM sessionized
        GROUP BY
            data_date,
            category,
            region,
            new_vs_returning_user
    )
    SELECT
        data_date,
        category,
        region,
        new_vs_returning_user,
        sessions,
        CAST(sessions AS DOUBLE) / CAST(GREATEST(distinct_active_users, 1) AS DOUBLE) AS sessions_per_user,
        avg_session_duration_sec,
        CAST(total_events AS DOUBLE) / CAST(GREATEST(sessions, 1) AS DOUBLE) AS events_per_session,
        CAST(total_watch_time_sum_ms AS DOUBLE) / CAST(GREATEST(sessions, 1) AS DOUBLE)
            AS watch_time_per_session_ms,
        current_timestamp() AS published_at
    FROM aggregated
    """.strip()
