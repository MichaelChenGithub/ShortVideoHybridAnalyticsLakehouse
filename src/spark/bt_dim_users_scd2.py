"""Batch transform for lakehouse.dims.dim_users_scd2."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

try:
    from spark.bt_dim_users_scd2_sql import (
        DIM_USERS_SCD2_TABLE,
        OPEN_END_VALID_TO,
        create_dim_users_scd2_sql,
        overwrite_dim_users_scd2_from_raw_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from bt_dim_users_scd2_sql import (
        DIM_USERS_SCD2_TABLE,
        OPEN_END_VALID_TO,
        create_dim_users_scd2_sql,
        overwrite_dim_users_scd2_from_raw_sql,
    )

RAW_CDC_TABLE = "lakehouse.bronze.raw_cdc_users"

ENV_SOURCE_TABLE = "BT_DIM_USERS_SCD2_SOURCE_TABLE"
ENV_TARGET_TABLE = "BT_DIM_USERS_SCD2_TARGET_TABLE"
ENV_APP_NAME = "BT_DIM_USERS_SCD2_APP_NAME"


@dataclass(frozen=True)
class JobSettings:
    app_name: str
    source_table: str
    target_table: str


def load_job_settings(env: Mapping[str, str] | None = None) -> JobSettings:
    values = os.environ if env is None else env
    return JobSettings(
        app_name=values.get(ENV_APP_NAME, "spark_bt_dim_users_scd2"),
        source_table=values.get(ENV_SOURCE_TABLE, RAW_CDC_TABLE),
        target_table=values.get(ENV_TARGET_TABLE, DIM_USERS_SCD2_TABLE),
    )


def run_batch(settings: JobSettings) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(settings.app_name).getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dims")
    spark.sql(create_dim_users_scd2_sql(settings.target_table))

    source_row_count = spark.read.format("iceberg").load(settings.source_table).count()

    spark.sql(
        overwrite_dim_users_scd2_from_raw_sql(
            source_table=settings.source_table,
            target_table=settings.target_table,
            open_end_valid_to=OPEN_END_VALID_TO,
        )
    )

    output_row_count = spark.read.format("iceberg").load(settings.target_table).count()
    print(
        "Built dim_users_scd2 rows: "
        f"source_rows={source_row_count}, output_rows={output_row_count}"
    )


def main() -> int:
    settings = load_job_settings()
    run_batch(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
