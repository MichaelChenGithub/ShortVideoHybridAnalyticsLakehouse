"""Batch transform for lakehouse.silver.user_activity_sessions_30m."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

try:
    from spark.bt_user_activity_sessions_30m_sql import (
        DIM_USERS_SCD2_TABLE,
        EVENTS_CONFORMED_TABLE,
        USER_ACTIVITY_SESSIONS_30M_TABLE,
        create_user_activity_sessions_30m_sql,
        default_d_minus_1_data_date_sql,
        delete_user_activity_sessions_30m_data_date_sql,
        insert_user_activity_sessions_30m_for_data_date_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from bt_user_activity_sessions_30m_sql import (
        DIM_USERS_SCD2_TABLE,
        EVENTS_CONFORMED_TABLE,
        USER_ACTIVITY_SESSIONS_30M_TABLE,
        create_user_activity_sessions_30m_sql,
        default_d_minus_1_data_date_sql,
        delete_user_activity_sessions_30m_data_date_sql,
        insert_user_activity_sessions_30m_for_data_date_sql,
    )

ENV_SOURCE_TABLE = "BT_USER_ACTIVITY_SESSIONS_30M_SOURCE_TABLE"
ENV_DIM_USERS_TABLE = "BT_USER_ACTIVITY_SESSIONS_30M_DIM_USERS_TABLE"
ENV_TARGET_TABLE = "BT_USER_ACTIVITY_SESSIONS_30M_TARGET_TABLE"
ENV_APP_NAME = "BT_USER_ACTIVITY_SESSIONS_30M_APP_NAME"
ENV_DATA_DATE = "BT_USER_ACTIVITY_SESSIONS_30M_DATA_DATE"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    source_table: str
    dim_users_table: str
    target_table: str
    data_date_sql: str | None


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    data_date = values.get(ENV_DATA_DATE)
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, "spark_bt_user_activity_sessions_30m"),
        source_table=values.get(ENV_SOURCE_TABLE, EVENTS_CONFORMED_TABLE),
        dim_users_table=values.get(ENV_DIM_USERS_TABLE, DIM_USERS_SCD2_TABLE),
        target_table=values.get(ENV_TARGET_TABLE, USER_ACTIVITY_SESSIONS_30M_TABLE),
        data_date_sql=(f"DATE '{data_date}'" if data_date else None),
    )


def run_batch(settings: JobSettings) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(settings.app_name).getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(create_user_activity_sessions_30m_sql(settings.target_table))
    spark.sql(
        delete_user_activity_sessions_30m_data_date_sql(
            target_table=settings.target_table,
            data_date_sql=settings.data_date_sql,
        )
    )
    spark.sql(
        insert_user_activity_sessions_30m_for_data_date_sql(
            source_table=settings.source_table,
            dim_users_table=settings.dim_users_table,
            target_table=settings.target_table,
            data_date_sql=settings.data_date_sql,
        )
    )

    target_data_date = settings.data_date_sql or default_d_minus_1_data_date_sql()
    print(
        "Built user_activity_sessions_30m for data_date="
        f"{target_data_date} "
        f"from source={settings.source_table} "
        f"using dim_users={settings.dim_users_table} "
        f"to target={settings.target_table}"
    )


def main() -> int:
    settings = load_job_settings()
    run_batch(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
