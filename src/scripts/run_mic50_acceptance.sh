#!/usr/bin/env bash
set -euo pipefail

SPARK_CONTAINER="${SPARK_CONTAINER:-lakehouse-spark}"
TABLE_NAME="${TABLE_NAME:-lakehouse.dims.rt_rule_quantile_baselines}"
RESET_TABLE="${RESET_TABLE:-1}"

printf '[MIC-50] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest spark

if [ "$RESET_TABLE" = "1" ]; then
  printf '[MIC-50] Resetting table for deterministic local acceptance...\n'
  docker exec -i -e MIC50_TABLE="$TABLE_NAME" "$SPARK_CONTAINER" python - <<'PY'
from __future__ import annotations

import os

from pyspark.sql import SparkSession

table_name = os.environ["MIC50_TABLE"]
spark = SparkSession.builder.getOrCreate()
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"[MIC-50] Dropped table if existed: {table_name}")
PY
fi

printf '[MIC-50] Publishing global-only baseline rows via Spark publisher...\n'
docker exec "$SPARK_CONTAINER" python /home/iceberg/local/src/spark/rt_rule_quantile_baselines_publish.py \
  --table "$TABLE_NAME"

printf '[MIC-50] Re-running publisher to validate idempotency...\n'
docker exec "$SPARK_CONTAINER" python /home/iceberg/local/src/spark/rt_rule_quantile_baselines_publish.py \
  --table "$TABLE_NAME"

printf '[MIC-50] Running validation checks...\n'
docker exec -i -e MIC50_TABLE="$TABLE_NAME" "$SPARK_CONTAINER" python - <<'PY'
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/home/iceberg/local/src")
from spark.rt_rule_quantile_baselines_sql import (  # noqa: E402
    M1_EFFECTIVE_FROM,
    M1_EFFECTIVE_TO,
    M1_RULE_VERSION,
)

table_name = os.environ["MIC50_TABLE"]
parts = table_name.split(".")
if len(parts) != 3:
    raise ValueError(f"Expected 3-part table name, got: {table_name}")
catalog_name, schema_name, relation_name = parts

spark = SparkSession.builder.getOrCreate()
published_filter = (
    f"rule_version = '{M1_RULE_VERSION}' "
    f"AND effective_from = DATE '{M1_EFFECTIVE_FROM}' "
    f"AND effective_to = DATE '{M1_EFFECTIVE_TO}'"
)

published_count = spark.sql(
    f"""
    SELECT COUNT(*) AS row_count
    FROM {table_name}
    WHERE {published_filter}
    """
).collect()[0]["row_count"]
if published_count != 2:
    raise AssertionError(f"Expected 2 global-only rows for M1 published set, found {published_count}")

table_exists = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").where(
    f"tableName = '{relation_name}'"
).count()
if table_exists != 1:
    raise AssertionError(f"Table {table_name} does not exist or is not queryable")

null_count = spark.sql(
    f"""
    SELECT COUNT(*) AS null_count
    FROM {table_name}
    WHERE {published_filter}
      AND (threshold_value IS NULL OR sample_size IS NULL)
    """
).collect()[0]["null_count"]
if null_count != 0:
    raise AssertionError(f"Published rows contain null threshold/sample values: {null_count}")

global_velocity = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND metric_name = 'velocity_30m'
      AND percentile = 90
      AND cohort_category IS NULL
      AND cohort_region IS NULL
      AND sample_size >= 1000
    """
).collect()[0]["combo_count"]
if global_velocity != 1:
    raise AssertionError("Missing required global velocity p90 baseline row with guard")

global_impressions = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND metric_name = 'impressions_30m'
      AND percentile = 40
      AND cohort_category IS NULL
      AND cohort_region IS NULL
      AND sample_size >= 1000
    """
).collect()[0]["combo_count"]
if global_impressions != 1:
    raise AssertionError("Missing required global impressions p40 baseline row with guard")

cohort_rows = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND (cohort_category IS NOT NULL OR cohort_region IS NOT NULL)
    """
).collect()[0]["combo_count"]
if cohort_rows != 0:
    raise AssertionError("M1 global-only scope expects no cohort rows in published set")

global_min_sample = spark.sql(
    f"""
    SELECT MIN(sample_size) AS min_sample
    FROM {table_name}
    WHERE {published_filter}
      AND cohort_category IS NULL
      AND cohort_region IS NULL
    """
).collect()[0]["min_sample"]
if global_min_sample is None or global_min_sample < 1000:
    raise AssertionError(
        "Global publish guard violated: expected min global sample_size >= 1000"
    )

print("[MIC-50] Validation checks passed.")
PY

printf '[MIC-50] Acceptance flow completed.\n'
