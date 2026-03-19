"""SQL builders for batch dim_videos_scd2 contract."""

from __future__ import annotations

from typing import Tuple

DIM_VIDEOS_SCD2_TABLE = "lakehouse.dims.dim_videos_scd2"
OPEN_END_VALID_TO = "9999-12-31 00:00:00"

_REQUIRED_DIM_VIDEOS_SCD2_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("video_sk", "STRING"),
    ("video_id", "STRING"),
    ("category", "STRING"),
    ("region", "STRING"),
    ("status", "STRING"),
    ("valid_from", "TIMESTAMP"),
    ("valid_to", "TIMESTAMP"),
    ("is_current", "BOOLEAN"),
)


def required_dim_videos_scd2_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_DIM_VIDEOS_SCD2_COLUMNS


def create_dim_videos_scd2_sql(table_name: str = DIM_VIDEOS_SCD2_TABLE) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        video_sk STRING,
        video_id STRING,
        category STRING,
        region STRING,
        status STRING,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    ) USING iceberg
    PARTITIONED BY (days(valid_from), bucket(64, video_id))
    TBLPROPERTIES (
        'format-version'='2'
    )
    """.strip()


def overwrite_dim_videos_scd2_from_raw_sql(
    *,
    source_table: str,
    target_table: str = DIM_VIDEOS_SCD2_TABLE,
    open_end_valid_to: str = OPEN_END_VALID_TO,
) -> str:
    return f"""
    INSERT OVERWRITE {target_table}
    WITH source_filtered AS (
        SELECT
            op,
            ts_ms,
            video_id,
            category,
            region,
            status,
            source_partition,
            source_offset
        FROM {source_table}
        WHERE op IN ('c', 'u')
          AND video_id IS NOT NULL
          AND ts_ms IS NOT NULL
    ),
    deduped AS (
        SELECT
            op,
            ts_ms,
            video_id,
            category,
            region,
            status,
            source_partition,
            source_offset
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY video_id, ts_ms
                    ORDER BY source_partition DESC NULLS LAST, source_offset DESC NULLS LAST
                ) AS rn
            FROM source_filtered
        ) ranked
        WHERE rn = 1
    ),
    with_prev AS (
        SELECT
            *,
            LAG(category) OVER (
                PARTITION BY video_id
                ORDER BY ts_ms ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS prev_category,
            LAG(region) OVER (
                PARTITION BY video_id
                ORDER BY ts_ms ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS prev_region,
            LAG(status) OVER (
                PARTITION BY video_id
                ORDER BY ts_ms ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS prev_status
        FROM deduped
    ),
    changed AS (
        SELECT
            *,
            CAST((ts_ms / 1000.0) AS TIMESTAMP) AS valid_from
        FROM with_prev
        WHERE prev_category IS NULL
           OR NOT (
                category <=> prev_category
            AND region <=> prev_region
            AND status <=> prev_status
           )
    ),
    with_next AS (
        SELECT
            *,
            LEAD(valid_from) OVER (
                PARTITION BY video_id
                ORDER BY valid_from ASC, source_partition ASC NULLS FIRST, source_offset ASC NULLS FIRST
            ) AS next_valid_from
        FROM changed
    )
    SELECT
        SHA2(CONCAT_WS('|', video_id, CAST(ts_ms AS STRING)), 256) AS video_sk,
        video_id,
        category,
        region,
        status,
        valid_from,
        COALESCE(next_valid_from, TO_TIMESTAMP('{open_end_valid_to}')) AS valid_to,
        CASE WHEN next_valid_from IS NULL THEN true ELSE false END AS is_current
    FROM with_next
    """.strip()
