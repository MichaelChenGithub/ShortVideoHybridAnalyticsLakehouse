"""Batch transform for lakehouse.gold.batch_engagement_daily."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

try:
    from spark.bt_engagement_daily_sql import (
        BATCH_ENGAGEMENT_DAILY_TABLE,
        DIM_USERS_SCD2_TABLE,
        DIM_VIDEOS_SCD2_TABLE,
        EVENTS_CONFORMED_TABLE,
        create_batch_engagement_daily_sql,
        default_d_minus_1_data_date_sql,
        delete_batch_engagement_daily_data_date_sql,
        insert_batch_engagement_daily_for_data_date_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from bt_engagement_daily_sql import (
        BATCH_ENGAGEMENT_DAILY_TABLE,
        DIM_USERS_SCD2_TABLE,
        DIM_VIDEOS_SCD2_TABLE,
        EVENTS_CONFORMED_TABLE,
        create_batch_engagement_daily_sql,
        default_d_minus_1_data_date_sql,
        delete_batch_engagement_daily_data_date_sql,
        insert_batch_engagement_daily_for_data_date_sql,
    )

ENV_SOURCE_TABLE = "BT_ENGAGEMENT_DAILY_SOURCE_TABLE"
ENV_USERS_DIM_TABLE = "BT_ENGAGEMENT_DAILY_USERS_DIM_TABLE"
ENV_VIDEOS_DIM_TABLE = "BT_ENGAGEMENT_DAILY_VIDEOS_DIM_TABLE"
ENV_TARGET_TABLE = "BT_ENGAGEMENT_DAILY_TARGET_TABLE"
ENV_APP_NAME = "BT_ENGAGEMENT_DAILY_APP_NAME"
ENV_DATA_DATE = "BT_ENGAGEMENT_DAILY_DATA_DATE"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    source_table: str
    users_dim_table: str
    videos_dim_table: str
    target_table: str
    data_date_sql: str | None


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    data_date = values.get(ENV_DATA_DATE)
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, "spark_bt_engagement_daily"),
        source_table=values.get(ENV_SOURCE_TABLE, EVENTS_CONFORMED_TABLE),
        users_dim_table=values.get(ENV_USERS_DIM_TABLE, DIM_USERS_SCD2_TABLE),
        videos_dim_table=values.get(ENV_VIDEOS_DIM_TABLE, DIM_VIDEOS_SCD2_TABLE),
        target_table=values.get(ENV_TARGET_TABLE, BATCH_ENGAGEMENT_DAILY_TABLE),
        data_date_sql=(f"DATE '{data_date}'" if data_date else None),
    )


def run_batch(settings: JobSettings) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(settings.app_name).getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(create_batch_engagement_daily_sql(settings.target_table))
    spark.sql(
        delete_batch_engagement_daily_data_date_sql(
            target_table=settings.target_table,
            data_date_sql=settings.data_date_sql,
        )
    )
    spark.sql(
        insert_batch_engagement_daily_for_data_date_sql(
            source_table=settings.source_table,
            users_dim_table=settings.users_dim_table,
            videos_dim_table=settings.videos_dim_table,
            target_table=settings.target_table,
            data_date_sql=settings.data_date_sql,
        )
    )

    target_data_date = settings.data_date_sql or default_d_minus_1_data_date_sql()
    print(
        "Built batch_engagement_daily for data_date="
        f"{target_data_date} "
        f"from source={settings.source_table} "
        f"users_dim={settings.users_dim_table} "
        f"videos_dim={settings.videos_dim_table} "
        f"to target={settings.target_table}"
    )


def main() -> int:
    settings = load_job_settings()
    run_batch(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
