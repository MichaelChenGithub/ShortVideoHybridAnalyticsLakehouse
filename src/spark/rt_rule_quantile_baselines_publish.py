"""Publish M1 global-only quantile baselines into Iceberg."""

from __future__ import annotations

import argparse
from typing import Tuple

from pyspark.sql import SparkSession

try:
    from spark.rt_rule_quantile_baselines_sql import (
        M1_EFFECTIVE_FROM,
        M1_EFFECTIVE_TO,
        M1_RULE_VERSION,
        RT_RULE_QUANTILE_BASELINES_TABLE,
        create_rt_rule_quantile_baselines_sql,
        publish_rt_rules_v1_seed_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_rule_quantile_baselines_sql import (  # type: ignore
        M1_EFFECTIVE_FROM,
        M1_EFFECTIVE_TO,
        M1_RULE_VERSION,
        RT_RULE_QUANTILE_BASELINES_TABLE,
        create_rt_rule_quantile_baselines_sql,
        publish_rt_rules_v1_seed_sql,
    )


def _published_filter() -> str:
    return (
        f"rule_version = '{M1_RULE_VERSION}' "
        f"AND effective_from = DATE '{M1_EFFECTIVE_FROM}' "
        f"AND effective_to = DATE '{M1_EFFECTIVE_TO}'"
    )


def _split_table_name(table_name: str) -> Tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected 3-part table name, got: {table_name}")
    return parts[0], parts[1], parts[2]


def publish_global_m1_baselines(
    spark: SparkSession,
    table_name: str = RT_RULE_QUANTILE_BASELINES_TABLE,
) -> None:
    catalog_name, schema_name, _ = _split_table_name(table_name)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(create_rt_rule_quantile_baselines_sql(table_name))
    spark.sql(publish_rt_rules_v1_seed_sql(table_name))

    published_filter = _published_filter()
    total_rows = spark.sql(
        f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE {published_filter}"
    ).collect()[0]["row_count"]
    if total_rows != 2:
        raise RuntimeError(f"Expected 2 global-only published rows, found {total_rows}")

    cohort_rows = spark.sql(
        f"""
        SELECT COUNT(*) AS row_count
        FROM {table_name}
        WHERE {published_filter}
          AND (cohort_category IS NOT NULL OR cohort_region IS NOT NULL)
        """
    ).collect()[0]["row_count"]
    if cohort_rows != 0:
        raise RuntimeError(
            f"M1 global-only publish expects no cohort rows, found {cohort_rows}"
        )

    min_global_sample = spark.sql(
        f"""
        SELECT MIN(sample_size) AS min_sample
        FROM {table_name}
        WHERE {published_filter}
          AND cohort_category IS NULL
          AND cohort_region IS NULL
        """
    ).collect()[0]["min_sample"]
    if min_global_sample is None or min_global_sample < 1000:
        raise RuntimeError(
            "Global publish guard violated: expected min global sample_size >= 1000"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish M1 global-only baselines to rt_rule_quantile_baselines",
    )
    parser.add_argument(
        "--table",
        default=RT_RULE_QUANTILE_BASELINES_TABLE,
        help="Fully-qualified Iceberg table name",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    spark = SparkSession.builder.appName("rt_rule_quantile_baselines_publish").getOrCreate()
    publish_global_m1_baselines(spark, args.table)
    print(f"[MIC-50] Publish completed for table: {args.table}")


if __name__ == "__main__":
    main()
