"""SQL builders for batch user_activity_sessions_30m contract."""

from __future__ import annotations

from typing import Tuple

USER_ACTIVITY_SESSIONS_30M_TABLE = "lakehouse.silver.user_activity_sessions_30m"
EVENTS_CONFORMED_TABLE = "lakehouse.silver.events_conformed"
DIM_USERS_SCD2_TABLE = "lakehouse.dims.dim_users_scd2"
ET_TIMEZONE = "America/New_York"

_REQUIRED_USER_ACTIVITY_SESSIONS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("session_id", "STRING"),
    ("user_id", "STRING"),
    ("session_start_ts", "TIMESTAMP"),
    ("session_end_ts", "TIMESTAMP"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("session_duration_sec", "BIGINT"),
    ("event_count", "BIGINT"),
    ("watch_time_sum_ms", "BIGINT"),
    ("data_date", "DATE"),
)


def required_user_activity_sessions_30m_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_USER_ACTIVITY_SESSIONS_COLUMNS


def create_user_activity_sessions_30m_sql(
    table_name: str = USER_ACTIVITY_SESSIONS_30M_TABLE,
) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        session_id STRING,
        user_id STRING,
        session_start_ts TIMESTAMP,
        session_end_ts TIMESTAMP,
        category STRING,
        region STRING,
        new_vs_returning_user STRING,
        session_duration_sec BIGINT,
        event_count BIGINT,
        watch_time_sum_ms BIGINT,
        data_date DATE
    ) USING iceberg
    PARTITIONED BY (data_date, bucket(64, user_id))
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def default_d_minus_1_data_date_sql() -> str:
    return f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)"


def delete_user_activity_sessions_30m_data_date_sql(
    *,
    target_table: str = USER_ACTIVITY_SESSIONS_30M_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    DELETE FROM {target_table}
    WHERE data_date = {target_data_date}
    """.strip()


def insert_user_activity_sessions_30m_for_data_date_sql(
    *,
    source_table: str = EVENTS_CONFORMED_TABLE,
    dim_users_table: str = DIM_USERS_SCD2_TABLE,
    target_table: str = USER_ACTIVITY_SESSIONS_30M_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()

    return f"""
    INSERT INTO {target_table}
    WITH source_filtered AS (
        SELECT
            event_id,
            event_timestamp,
            data_date,
            video_id,
            user_id,
            watch_time_ms,
            category,
            region
        FROM {source_table}
        WHERE data_date = {target_data_date}
          AND event_id IS NOT NULL
          AND event_timestamp IS NOT NULL
          AND user_id IS NOT NULL
    ),
    with_prev AS (
        SELECT
            *,
            LAG(event_timestamp) OVER (
                PARTITION BY user_id, data_date
                ORDER BY event_timestamp ASC, event_id ASC
            ) AS prev_event_timestamp
        FROM source_filtered
    ),
    flagged AS (
        SELECT
            *,
            CASE
                WHEN prev_event_timestamp IS NULL THEN 1
                WHEN event_timestamp > prev_event_timestamp + INTERVAL 30 MINUTES THEN 1
                ELSE 0
            END AS is_new_session
        FROM with_prev
    ),
    sessionized AS (
        SELECT
            *,
            SUM(is_new_session) OVER (
                PARTITION BY user_id, data_date
                ORDER BY event_timestamp ASC, event_id ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS session_seq
        FROM flagged
    ),
    aggregated AS (
        SELECT
            user_id,
            data_date,
            session_seq,
            MIN(event_timestamp) AS session_start_ts,
            MAX(event_timestamp) AS session_end_ts,
            CAST(unix_timestamp(MAX(event_timestamp)) - unix_timestamp(MIN(event_timestamp)) AS BIGINT)
                AS session_duration_sec,
            COUNT(*) AS event_count,
            CAST(COALESCE(SUM(watch_time_ms), 0) AS BIGINT) AS watch_time_sum_ms
        FROM sessionized
        GROUP BY user_id, data_date, session_seq
    ),
    dominant_category_region AS (
        SELECT
            user_id,
            data_date,
            session_seq,
            category,
            region
        FROM (
            SELECT
                user_id,
                data_date,
                session_seq,
                category,
                region,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, data_date, session_seq
                    ORDER BY pair_watch_time_sum_ms DESC,
                             pair_event_count DESC,
                             pair_last_event_timestamp DESC,
                             pair_last_event_id DESC
                ) AS rn
            FROM (
                SELECT
                    user_id,
                    data_date,
                    session_seq,
                    category,
                    region,
                    CAST(COALESCE(SUM(watch_time_ms), 0) AS BIGINT) AS pair_watch_time_sum_ms,
                    COUNT(*) AS pair_event_count,
                    MAX(event_timestamp) AS pair_last_event_timestamp,
                    MAX_BY(event_id, STRUCT(event_timestamp, event_id)) AS pair_last_event_id
                FROM sessionized
                GROUP BY user_id, data_date, session_seq, category, region
            ) ranked_pairs
        ) ranked
        WHERE rn = 1
    ),
    session_user_attr AS (
        SELECT
            ranked.user_id,
            ranked.data_date,
            ranked.session_seq,
            COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user
        FROM (
            SELECT
                aggregated.user_id,
                aggregated.data_date,
                aggregated.session_seq,
                dim_users.new_vs_returning_user,
                ROW_NUMBER() OVER (
                    PARTITION BY aggregated.user_id, aggregated.data_date, aggregated.session_seq
                    ORDER BY dim_users.valid_from DESC NULLS LAST
                ) AS rn
            FROM aggregated
            LEFT JOIN {dim_users_table} dim_users
              ON aggregated.user_id = dim_users.user_id
             AND aggregated.session_end_ts >= dim_users.valid_from
             AND aggregated.session_end_ts < dim_users.valid_to
        ) ranked
        WHERE rn = 1
    )
    SELECT
        SHA2(CONCAT_WS('|', aggregated.user_id, CAST(aggregated.session_start_ts AS STRING)), 256) AS session_id,
        aggregated.user_id,
        aggregated.session_start_ts,
        aggregated.session_end_ts,
        dominant.category,
        dominant.region,
        user_attr.new_vs_returning_user,
        aggregated.session_duration_sec,
        aggregated.event_count,
        aggregated.watch_time_sum_ms,
        aggregated.data_date
    FROM aggregated
    INNER JOIN dominant_category_region dominant
      ON aggregated.user_id = dominant.user_id
     AND aggregated.data_date = dominant.data_date
     AND aggregated.session_seq = dominant.session_seq
    INNER JOIN session_user_attr user_attr
      ON aggregated.user_id = user_attr.user_id
     AND aggregated.data_date = user_attr.data_date
     AND aggregated.session_seq = user_attr.session_seq
    """.strip()
