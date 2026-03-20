"""Batch transform for lakehouse.silver.events_conformed."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

try:
    from spark.bt_events_conformed_sql import (
        EVENTS_CONFORMED_TABLE,
        RAW_EVENTS_TABLE,
        create_events_conformed_sql,
        default_d_minus_1_data_date_sql,
        delete_events_conformed_data_date_sql,
        insert_events_conformed_for_data_date_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from bt_events_conformed_sql import (
        EVENTS_CONFORMED_TABLE,
        RAW_EVENTS_TABLE,
        create_events_conformed_sql,
        default_d_minus_1_data_date_sql,
        delete_events_conformed_data_date_sql,
        insert_events_conformed_for_data_date_sql,
    )

ENV_SOURCE_TABLE = "BT_EVENTS_CONFORMED_SOURCE_TABLE"
ENV_TARGET_TABLE = "BT_EVENTS_CONFORMED_TARGET_TABLE"
ENV_APP_NAME = "BT_EVENTS_CONFORMED_APP_NAME"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    source_table: str
    target_table: str


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, "spark_bt_events_conformed"),
        source_table=values.get(ENV_SOURCE_TABLE, RAW_EVENTS_TABLE),
        target_table=values.get(ENV_TARGET_TABLE, EVENTS_CONFORMED_TABLE),
    )


def run_batch(settings: JobSettings) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(settings.app_name).getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(create_events_conformed_sql(settings.target_table))
    spark.sql(delete_events_conformed_data_date_sql(target_table=settings.target_table))
    spark.sql(
        insert_events_conformed_for_data_date_sql(
            source_table=settings.source_table,
            target_table=settings.target_table,
        )
    )

    print(
        "Built events_conformed for data_date="
        f"{default_d_minus_1_data_date_sql()} "
        f"from source={settings.source_table} to target={settings.target_table}"
    )


def main() -> int:
    settings = load_job_settings()
    run_batch(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
