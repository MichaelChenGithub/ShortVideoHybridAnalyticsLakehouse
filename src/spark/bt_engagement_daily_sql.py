"""SQL builders for batch engagement_daily contract."""

from __future__ import annotations

from typing import Tuple

BATCH_ENGAGEMENT_DAILY_TABLE = "lakehouse.gold.batch_engagement_daily"
EVENTS_CONFORMED_TABLE = "lakehouse.silver.events_conformed"
DIM_USERS_SCD2_TABLE = "lakehouse.dims.dim_users_scd2"
DIM_VIDEOS_SCD2_TABLE = "lakehouse.dims.dim_videos_scd2"
ET_TIMEZONE = "America/New_York"

_REQUIRED_BATCH_ENGAGEMENT_DAILY_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("data_date", "DATE"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("impressions", "BIGINT"),
    ("play_start", "BIGINT"),
    ("play_finish", "BIGINT"),
    ("likes", "BIGINT"),
    ("shares", "BIGINT"),
    ("skips", "BIGINT"),
    ("play_start_rate", "DOUBLE"),
    ("completion_rate", "DOUBLE"),
    ("interaction_rate", "DOUBLE"),
    ("skip_rate", "DOUBLE"),
    ("published_at", "TIMESTAMP"),
)


def required_batch_engagement_daily_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_BATCH_ENGAGEMENT_DAILY_COLUMNS


def create_batch_engagement_daily_sql(
    table_name: str = BATCH_ENGAGEMENT_DAILY_TABLE,
) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        data_date DATE,
        category STRING,
        region STRING,
        new_vs_returning_user STRING,
        impressions BIGINT,
        play_start BIGINT,
        play_finish BIGINT,
        likes BIGINT,
        shares BIGINT,
        skips BIGINT,
        play_start_rate DOUBLE,
        completion_rate DOUBLE,
        interaction_rate DOUBLE,
        skip_rate DOUBLE,
        published_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (data_date)
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def default_d_minus_1_data_date_sql() -> str:
    return f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)"


def delete_batch_engagement_daily_data_date_sql(
    *,
    target_table: str = BATCH_ENGAGEMENT_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    DELETE FROM {target_table}
    WHERE data_date = {target_data_date}
    """.strip()


def insert_batch_engagement_daily_for_data_date_sql(
    *,
    source_table: str = EVENTS_CONFORMED_TABLE,
    users_dim_table: str = DIM_USERS_SCD2_TABLE,
    videos_dim_table: str = DIM_VIDEOS_SCD2_TABLE,
    target_table: str = BATCH_ENGAGEMENT_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    INSERT INTO {target_table}
    WITH target_events AS (
        SELECT
            event_id,
            event_timestamp,
            data_date,
            video_id,
            user_id,
            event_type
        FROM {source_table}
        WHERE data_date = {target_data_date}
    ),
    user_attributed AS (
        SELECT
            event_id,
            COALESCE(region, 'unknown') AS region,
            COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user
        FROM (
            SELECT
                e.event_id,
                u.region,
                u.new_vs_returning_user,
                ROW_NUMBER() OVER (
                    PARTITION BY e.event_id
                    ORDER BY u.valid_from DESC NULLS LAST, u.valid_to ASC NULLS LAST
                ) AS rn
            FROM target_events e
            LEFT JOIN {users_dim_table} u
              ON e.user_id = u.user_id
             AND e.event_timestamp >= u.valid_from
             AND e.event_timestamp < u.valid_to
        ) ranked
        WHERE rn = 1
    ),
    video_attributed AS (
        SELECT
            event_id,
            COALESCE(category, 'unknown') AS category
        FROM (
            SELECT
                e.event_id,
                v.category,
                ROW_NUMBER() OVER (
                    PARTITION BY e.event_id
                    ORDER BY v.valid_from DESC NULLS LAST, v.valid_to ASC NULLS LAST
                ) AS rn
            FROM target_events e
            LEFT JOIN {videos_dim_table} v
              ON e.video_id = v.video_id
             AND e.event_timestamp >= v.valid_from
             AND e.event_timestamp < v.valid_to
        ) ranked
        WHERE rn = 1
    ),
    aggregated AS (
        SELECT
            e.data_date,
            v.category,
            u.region,
            u.new_vs_returning_user,
            SUM(CASE WHEN e.event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
            SUM(CASE WHEN e.event_type = 'play_start' THEN 1 ELSE 0 END) AS play_start,
            SUM(CASE WHEN e.event_type = 'play_finish' THEN 1 ELSE 0 END) AS play_finish,
            SUM(CASE WHEN e.event_type = 'like' THEN 1 ELSE 0 END) AS likes,
            SUM(CASE WHEN e.event_type = 'share' THEN 1 ELSE 0 END) AS shares,
            SUM(CASE WHEN e.event_type = 'skip' THEN 1 ELSE 0 END) AS skips
        FROM target_events e
        LEFT JOIN user_attributed u
          ON e.event_id = u.event_id
        LEFT JOIN video_attributed v
          ON e.event_id = v.event_id
        GROUP BY
            e.data_date,
            v.category,
            u.region,
            u.new_vs_returning_user
    )
    SELECT
        data_date,
        category,
        region,
        new_vs_returning_user,
        impressions,
        play_start,
        play_finish,
        likes,
        shares,
        skips,
        CAST(play_start AS DOUBLE) / CAST(GREATEST(impressions, 1) AS DOUBLE) AS play_start_rate,
        CAST(play_finish AS DOUBLE) / CAST(GREATEST(play_start, 1) AS DOUBLE) AS completion_rate,
        CAST((likes + shares) AS DOUBLE) / CAST(GREATEST(play_finish, 1) AS DOUBLE) AS interaction_rate,
        CAST(skips AS DOUBLE) / CAST(GREATEST(play_start, 1) AS DOUBLE) AS skip_rate,
        current_timestamp() AS published_at
    FROM aggregated
    """.strip()
