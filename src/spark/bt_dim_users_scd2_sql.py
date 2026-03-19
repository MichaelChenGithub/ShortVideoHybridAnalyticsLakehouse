"""SQL builders for batch dim_users_scd2 contract."""

from __future__ import annotations

from typing import Tuple

DIM_USERS_SCD2_TABLE = "lakehouse.dims.dim_users_scd2"
OPEN_END_VALID_TO = "9999-12-31 00:00:00"

_REQUIRED_DIM_USERS_SCD2_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("user_sk", "STRING"),
    ("user_id", "STRING"),
    ("region", "STRING"),
    ("new_vs_returning_user", "STRING"),
    ("valid_from", "TIMESTAMP"),
    ("valid_to", "TIMESTAMP"),
    ("is_current", "BOOLEAN"),
)


def required_dim_users_scd2_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_DIM_USERS_SCD2_COLUMNS


def create_dim_users_scd2_sql(table_name: str = DIM_USERS_SCD2_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        user_sk STRING,
        user_id STRING,
        region STRING,
        new_vs_returning_user STRING,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    ) USING iceberg
    PARTITIONED BY (days(valid_from), bucket(64, user_id))
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def overwrite_dim_users_scd2_from_raw_sql(
    *,
    source_table: str,
    target_table: str = DIM_USERS_SCD2_TABLE,
    open_end_valid_to: str = OPEN_END_VALID_TO,
) -> str:
    return f"""
    INSERT OVERWRITE {target_table}
    WITH source_filtered AS (
        SELECT
            op,
            ts_ms,
            user_id,
            region,
            LOWER(TRIM(new_vs_returning_user)) AS new_vs_returning_user,
            source_partition,
            source_offset
        FROM {source_table}
        WHERE op IN ('c', 'u')
          AND user_id IS NOT NULL
          AND ts_ms IS NOT NULL
          AND region IS NOT NULL
          AND new_vs_returning_user IS NOT NULL
          AND LOWER(TRIM(new_vs_returning_user)) IN ('new', 'returning', 'unknown')
    ),
    deduped AS (
        SELECT
            op,
            ts_ms,
            user_id,
            region,
            new_vs_returning_user,
            source_partition,
            source_offset
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, ts_ms
                    ORDER BY source_partition DESC NULLS LAST, source_offset DESC NULLS LAST
                ) AS rn
            FROM source_filtered
        ) ranked
        WHERE rn = 1
    ),
    with_prev AS (
        SELECT
            *,
            LAG(region) OVER (
                PARTITION BY user_id
                ORDER BY ts_ms ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS prev_region,
            LAG(new_vs_returning_user) OVER (
                PARTITION BY user_id
                ORDER BY ts_ms ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS prev_new_vs_returning_user
        FROM deduped
    ),
    changed AS (
        SELECT
            *,
            CAST((ts_ms / 1000.0) AS TIMESTAMP) AS valid_from
        FROM with_prev
        WHERE prev_region IS NULL
           OR prev_new_vs_returning_user IS NULL
           OR NOT (
                region <=> prev_region
            AND new_vs_returning_user <=> prev_new_vs_returning_user
           )
    ),
    with_next AS (
        SELECT
            *,
            LEAD(valid_from) OVER (
                PARTITION BY user_id
                ORDER BY valid_from ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS next_valid_from
        FROM changed
    )
    SELECT
        SHA2(CONCAT_WS('|', user_id, CAST(ts_ms AS STRING)), 256) AS user_sk,
        user_id,
        region,
        new_vs_returning_user,
        valid_from,
        COALESCE(next_valid_from, TO_TIMESTAMP('{open_end_valid_to}')) AS valid_to,
        CASE WHEN next_valid_from IS NULL THEN true ELSE false END AS is_current
    FROM with_next
    """.strip()
