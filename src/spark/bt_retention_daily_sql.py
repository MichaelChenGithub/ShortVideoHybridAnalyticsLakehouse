"""SQL builders for batch retention_daily contract."""

from __future__ import annotations

from typing import Tuple

EVENTS_CONFORMED_TABLE = "lakehouse.silver.events_conformed"
DIM_USERS_SCD2_TABLE = "lakehouse.dims.dim_users_scd2"
DIM_VIDEOS_SCD2_TABLE = "lakehouse.dims.dim_videos_scd2"
BATCH_RETENTION_DAILY_TABLE = "lakehouse.gold.batch_retention_daily"
ET_TIMEZONE = "America/New_York"

_REQUIRED_BATCH_RETENTION_DAILY_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("cohort_date", "DATE"),
    ("day_n", "INT"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("cohort_users", "BIGINT"),
    ("retained_users", "BIGINT"),
    ("retention_rate", "DOUBLE"),
    ("data_date", "DATE"),
    ("published_at", "TIMESTAMP"),
)


def required_batch_retention_daily_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_BATCH_RETENTION_DAILY_COLUMNS


def create_batch_retention_daily_sql(table_name: str = BATCH_RETENTION_DAILY_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        cohort_date DATE,
        day_n INT,
        category STRING,
        region STRING,
        new_vs_returning_user STRING,
        cohort_users BIGINT,
        retained_users BIGINT,
        retention_rate DOUBLE,
        data_date DATE,
        published_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (data_date)
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def default_d_minus_1_data_date_sql() -> str:
    return f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)"


def delete_batch_retention_daily_data_date_sql(
    *,
    target_table: str = BATCH_RETENTION_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    DELETE FROM {target_table}
    WHERE data_date = {target_data_date}
    """.strip()


def insert_batch_retention_daily_for_data_date_sql(
    *,
    source_table: str = EVENTS_CONFORMED_TABLE,
    user_dim_table: str = DIM_USERS_SCD2_TABLE,
    video_dim_table: str = DIM_VIDEOS_SCD2_TABLE,
    target_table: str = BATCH_RETENTION_DAILY_TABLE,
    data_date_sql: str | None = None,
) -> str:
    target_data_date = data_date_sql or default_d_minus_1_data_date_sql()
    return f"""
    INSERT INTO {target_table}
    WITH horizons AS (
        SELECT 1 AS day_n
        UNION ALL
        SELECT 7 AS day_n
    ),
    cohort_events AS (
        SELECT
            h.day_n,
            date_sub({target_data_date}, h.day_n) AS cohort_date,
            e.event_timestamp,
            e.video_id,
            e.user_id,
            e.category AS event_category,
            e.region AS event_region
        FROM {source_table} e
        INNER JOIN horizons h
            ON e.event_date_et = date_sub({target_data_date}, h.day_n)
        WHERE e.user_id IS NOT NULL
          AND e.video_id IS NOT NULL
    ),
    cohort_attributed AS (
        SELECT DISTINCT
            c.cohort_date,
            c.day_n,
            c.user_id,
            COALESCE(NULLIF(TRIM(v.category), ''), NULLIF(TRIM(c.event_category), ''), 'unknown') AS category,
            COALESCE(NULLIF(TRIM(u.region), ''), NULLIF(TRIM(c.event_region), ''), 'unknown') AS region,
            COALESCE(NULLIF(LOWER(TRIM(u.new_vs_returning_user)), ''), 'unknown') AS new_vs_returning_user
        FROM cohort_events c
        LEFT JOIN {video_dim_table} v
            ON c.video_id = v.video_id
           AND c.event_timestamp >= v.valid_from
           AND c.event_timestamp < v.valid_to
        LEFT JOIN {user_dim_table} u
            ON c.user_id = u.user_id
           AND c.event_timestamp >= u.valid_from
           AND c.event_timestamp < u.valid_to
    ),
    cohort_users AS (
        SELECT
            cohort_date,
            day_n,
            category,
            region,
            new_vs_returning_user,
            COUNT(DISTINCT user_id) AS cohort_users
        FROM cohort_attributed
        GROUP BY 1, 2, 3, 4, 5
    ),
    retained_activity AS (
        SELECT DISTINCT
            user_id
        FROM {source_table}
        WHERE event_date_et = {target_data_date}
          AND user_id IS NOT NULL
    ),
    retained_users AS (
        SELECT
            c.cohort_date,
            c.day_n,
            c.category,
            c.region,
            c.new_vs_returning_user,
            COUNT(DISTINCT c.user_id) AS retained_users
        FROM cohort_attributed c
        INNER JOIN retained_activity r
            ON c.user_id = r.user_id
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT
        c.cohort_date,
        c.day_n,
        c.category,
        c.region,
        c.new_vs_returning_user,
        c.cohort_users,
        COALESCE(r.retained_users, 0) AS retained_users,
        CASE
            WHEN c.cohort_users > 0 THEN COALESCE(r.retained_users, 0) / CAST(c.cohort_users AS DOUBLE)
            ELSE NULL
        END AS retention_rate,
        {target_data_date} AS data_date,
        current_timestamp() AS published_at
    FROM cohort_users c
    LEFT JOIN retained_users r
        ON c.cohort_date = r.cohort_date
       AND c.day_n = r.day_n
       AND c.category = r.category
       AND c.region = r.region
       AND c.new_vs_returning_user = r.new_vs_returning_user
    """.strip()
