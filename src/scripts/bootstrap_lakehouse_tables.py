"""Bootstrap all Iceberg namespaces and tables before streaming/batch jobs start.

Imports DDL from the existing SQL modules so schemas stay in sync with the
jobs that populate them. Safe to re-run — all statements are IF NOT EXISTS.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from src/ when run via spark-submit from within the container.
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.bt_dim_users_scd2_sql import create_dim_users_scd2_sql
from spark.bt_dim_videos_scd2_sql import create_dim_videos_scd2_sql
from spark.bt_engagement_daily_sql import create_batch_engagement_daily_sql
from spark.bt_events_conformed_sql import create_events_conformed_sql
from spark.bt_retention_daily_sql import create_batch_retention_daily_sql
from spark.bt_sessionization_daily_sql import create_batch_sessionization_daily_sql
from spark.bt_user_activity_sessions_30m_sql import create_user_activity_sessions_30m_sql
from spark.rt_content_events_aggregator_sql import (
    create_invalid_events_content_sql,
    create_raw_events_sql,
    create_rt_video_stats_sql,
)
from spark.rt_rule_quantile_baselines_sql import create_rt_rule_quantile_baselines_sql
from spark.rt_user_cdc_raw_sql import (
    create_invalid_events_cdc_users_sql,
    create_raw_cdc_users_sql,
)
from spark.rt_video_cdc_upsert_sql import (
    create_dim_videos_sql,
    create_invalid_events_cdc_videos_sql,
    create_raw_cdc_videos_sql,
)

_NAMESPACES = (
    "lakehouse.bronze",
    "lakehouse.silver",
    "lakehouse.gold",
    "lakehouse.dims",
)

# (sql_builder_fn, human-readable table name for logging)
_TABLE_BUILDERS = (
    # bronze — raw ingest
    (create_raw_events_sql,               "lakehouse.bronze.raw_events"),
    (create_invalid_events_content_sql,   "lakehouse.bronze.invalid_events_content"),
    (create_raw_cdc_users_sql,            "lakehouse.bronze.raw_cdc_users"),
    (create_invalid_events_cdc_users_sql, "lakehouse.bronze.invalid_events_cdc_users"),
    (create_raw_cdc_videos_sql,           "lakehouse.bronze.raw_cdc_videos"),
    (create_invalid_events_cdc_videos_sql,"lakehouse.bronze.invalid_events_cdc_videos"),
    # dims — realtime + batch
    (create_dim_videos_sql,               "lakehouse.dims.dim_videos"),
    (create_rt_rule_quantile_baselines_sql,"lakehouse.dims.rt_rule_quantile_baselines"),
    (create_dim_users_scd2_sql,           "lakehouse.dims.dim_users_scd2"),
    (create_dim_videos_scd2_sql,          "lakehouse.dims.dim_videos_scd2"),
    # silver — batch conformed
    (create_events_conformed_sql,              "lakehouse.silver.events_conformed"),
    (create_user_activity_sessions_30m_sql,    "lakehouse.silver.user_activity_sessions_30m"),
    # gold — realtime
    (create_rt_video_stats_sql,           "lakehouse.gold.rt_video_stats_1min"),
    # gold — batch
    (create_batch_retention_daily_sql,    "lakehouse.gold.batch_retention_daily"),
    (create_batch_engagement_daily_sql,   "lakehouse.gold.batch_engagement_daily"),
    (create_batch_sessionization_daily_sql,"lakehouse.gold.batch_sessionization_daily"),
)


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bootstrap_lakehouse_tables").getOrCreate()

    for namespace in _NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        print(f"[bootstrap] namespace ready: {namespace}")

    for build_sql, table_name in _TABLE_BUILDERS:
        spark.sql(build_sql())
        print(f"[bootstrap] table ready: {table_name}")

    print("[bootstrap] done")


if __name__ == "__main__":
    main()
